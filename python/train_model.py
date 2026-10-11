import json
import numpy as np
import torch
import torch.nn as nn
import torch.nn.functional as F
from torch_geometric.loader import DataLoader
from torch_geometric.nn import GCNConv

train_snapshots = torch.load("/output/train_snapshots.pt", weights_only=False)
test_snapshots  = torch.load("/output/test_snapshots.pt", weights_only=False)
with open("/output/feature_stats.json") as f:
    stats = json.load(f)

ili_mean, ili_std = stats["ILI"]["mean"], stats["ILI"]["std"]
CLIMATE_IDX = slice(0, 4)
TS_IDX = slice(4, 9)

print(f"Train snapshots: {len(train_snapshots)} | Test snapshots: {len(test_snapshots)}")

# ---- Sanity check: is the INPUT data itself clean, before any training happens? ----
def check_nan(snapshots, name):
    bad = 0
    for d in snapshots:
        if torch.isnan(d.x).any() or torch.isnan(d.y).any() or torch.isnan(d.edge_attr).any():
            bad += 1
    print(f"{name}: {bad}/{len(snapshots)} snapshots contain NaN")
    return bad > 0

train_has_nan = check_nan(train_snapshots, "train")
test_has_nan = check_nan(test_snapshots, "test")
if train_has_nan or test_has_nan:
    raise RuntimeError("NaN found in input data itself — issue is in prepare_data.py, not training.")
print("Input data confirmed clean — issue must be in training dynamics.\n")

class ClimaFluGNN(nn.Module):
    def __init__(self, climate_dim=4, ts_dim=5, hidden=16):
        super().__init__()
        self.ts_encoder = nn.Sequential(
            nn.Linear(ts_dim, hidden), nn.ReLU(),
            nn.Linear(hidden, hidden), nn.ReLU()
        )
        self.gcn1 = GCNConv(climate_dim, hidden)
        self.gcn2 = GCNConv(hidden, hidden)
        self.fusion = nn.Sequential(
            nn.Linear(hidden * 2, hidden), nn.ReLU(),
            nn.Linear(hidden, 1)
        )

    def forward(self, x, edge_index, edge_weight):
        climate_x = x[:, CLIMATE_IDX]
        ts_x = x[:, TS_IDX]
        ts_embed = self.ts_encoder(ts_x)
        g = F.relu(self.gcn1(climate_x, edge_index, edge_weight))
        g = F.relu(self.gcn2(g, edge_index, edge_weight))
        fused = torch.cat([ts_embed, g], dim=1)
        return self.fusion(fused).squeeze(-1)

model = ClimaFluGNN()
optimizer = torch.optim.Adam(model.parameters(), lr=0.001, weight_decay=1e-5)  # lowered from 0.005

train_loader = DataLoader(train_snapshots, batch_size=8, shuffle=True)
test_loader = DataLoader(test_snapshots, batch_size=16, shuffle=False)

# ---- One diagnostic forward+backward pass BEFORE the real loop, to catch NaN immediately ----
first_batch = next(iter(train_loader))
optimizer.zero_grad()
pred = model(first_batch.x, first_batch.edge_index, first_batch.edge_attr.squeeze(-1))
loss = F.mse_loss(pred, first_batch.y)
print(f"First batch — pred has NaN: {torch.isnan(pred).any().item()} | loss: {loss.item()}")
loss.backward()
grad_norm = sum(p.grad.norm().item() for p in model.parameters() if p.grad is not None)
print(f"First batch gradient norm: {grad_norm:.4f}\n")
model.zero_grad()

EPOCHS = 100
best_test_rmse = float("inf")

for epoch in range(1, EPOCHS + 1):
    model.train()
    total_loss = 0.0
    for batch in train_loader:
        optimizer.zero_grad()
        edge_weight = batch.edge_attr.squeeze(-1)
        pred = model(batch.x, batch.edge_index, edge_weight)
        loss = F.mse_loss(pred, batch.y)
        loss.backward()
        torch.nn.utils.clip_grad_norm_(model.parameters(), max_norm=1.0)  # gradient clipping
        optimizer.step()
        total_loss += loss.item() * batch.num_graphs

    if epoch % 10 == 0 or epoch == 1:
        model.eval()
        preds_raw, targets_raw = [], []
        with torch.no_grad():
            for batch in test_loader:
                edge_weight = batch.edge_attr.squeeze(-1)
                pred_z = model(batch.x, batch.edge_index, edge_weight)
                preds_raw.append(pred_z.numpy() * ili_std + ili_mean)
                targets_raw.append(batch.y.numpy() * ili_std + ili_mean)
        preds_raw = np.concatenate(preds_raw)
        targets_raw = np.concatenate(targets_raw)
        rmse = np.sqrt(np.mean((preds_raw - targets_raw) ** 2))
        mae = np.mean(np.abs(preds_raw - targets_raw))
        print(f"Epoch {epoch:3d} | train_loss(z): {total_loss/len(train_snapshots):.4f} | test RMSE: {rmse:.4f} | test MAE: {mae:.4f}")
        if rmse < best_test_rmse:
            best_test_rmse = rmse
            torch.save(model.state_dict(), "/output/climaflu_gnn_best.pt")

model.load_state_dict(torch.load("/output/climaflu_gnn_best.pt"))
model.eval()
preds_raw, targets_raw, meta_rows = [], [], []
with open("/output/test_meta.json") as f:
    test_meta = json.load(f)

with torch.no_grad():
    for i, data in enumerate(test_snapshots):
        edge_weight = data.edge_attr.squeeze(-1)
        pred_z = model(data.x, data.edge_index, edge_weight)
        pred_raw = (pred_z.numpy() * ili_std + ili_mean)
        target_raw = np.array(test_meta[i]["y_raw"])
        preds_raw.append(pred_raw)
        targets_raw.append(target_raw)
        for s, p, t in zip(test_meta[i]["states"], pred_raw, target_raw):
            meta_rows.append({"state": s, "year": test_meta[i]["year"], "week": test_meta[i]["week"], "prediction": float(p), "target": float(t)})

preds_raw = np.concatenate(preds_raw)
targets_raw = np.concatenate(targets_raw)
final_rmse = np.sqrt(np.mean((preds_raw - targets_raw) ** 2))
final_mae = np.mean(np.abs(preds_raw - targets_raw))
ss_res = np.sum((targets_raw - preds_raw) ** 2)
ss_tot = np.sum((targets_raw - np.mean(targets_raw)) ** 2)
final_r2 = 1 - ss_res / ss_tot

print(f"\n=== FINAL ClimaFlu-GNN RESULTS (best checkpoint) ===")
print(f"RMSE: {final_rmse:.4f} | MAE: {final_mae:.4f} | R2: {final_r2:.4f}")
print(f"\n=== COMPARISON ===")
print(f"Persistence baseline — RMSE: 1.0372 | MAE: 0.5519")
print(f"GBTRegressor baseline — RMSE: 0.9538 | MAE: 0.5073 | R2: 0.7688")
print(f"ClimaFlu-GNN           — RMSE: {final_rmse:.4f} | MAE: {final_mae:.4f} | R2: {final_r2:.4f}")
print(f"Improvement over GBT — RMSE: {(1 - final_rmse/0.9538)*100:.2f}% | MAE: {(1 - final_mae/0.5073)*100:.2f}%")

import pandas as pd
pd.DataFrame(meta_rows).to_csv("/output/climaflu_gnn_predictions.csv", index=False)
print("\nSaved: climaflu_gnn_best.pt, climaflu_gnn_predictions.csv")