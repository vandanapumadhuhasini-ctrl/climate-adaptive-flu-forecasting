"""Rigour pass: deterministic Python baselines on the canonical feature table (docs/RESEARCH_PROTOCOL.md section 5).
  Persistence: y_hat = ILI_lag1        Ridge: alpha = 1.0 on all features, standardised on YEAR <= 2022
Writes results/rigour/predictions/{Persistence,Ridge}.parquet (same columns as the neural predictions).
Run: docker exec -w /workspace/rigour pytorch python baselines_v2.py
"""
import os

import pandas as pd
from sklearn.linear_model import Ridge
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from common import FEATURES, OUT, rmse

feat = pd.read_parquet(f"{OUT}/feature_table.parquet")
train, test = feat[feat.YEAR <= 2022], feat[feat.YEAR >= 2023]
os.makedirs(f"{OUT}/predictions", exist_ok=True)
base = test[["state_abbrev", "YEAR", "WEEK", "target"]].rename(columns={"state_abbrev": "state", "YEAR": "year", "WEEK": "week"})

base.assign(pred=test["ILI_lag1"].values).to_parquet(f"{OUT}/predictions/Persistence.parquet", index=False)
ridge = make_pipeline(StandardScaler(), Ridge(alpha=1.0)).fit(train[FEATURES], train["target"])
base.assign(pred=ridge.predict(test[FEATURES])).to_parquet(f"{OUT}/predictions/Ridge.parquet", index=False)
print("Persistence RMSE", round(rmse(test["ILI_lag1"], test["target"]), 4),
      "| Ridge RMSE", round(rmse(ridge.predict(test[FEATURES]), test["target"]), 4))
