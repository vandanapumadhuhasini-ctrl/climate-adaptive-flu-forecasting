import sys, time
import pandas as pd

n_years = int(sys.argv[1]) if len(sys.argv) > 1 else 1
years = list(range(2000, 2000 + n_years))
cols = ["id", "date", "element", "value", "mflag", "qflag", "sflag", "obstime"]

t0 = time.time()
frames = []
for y in years:
    path = f"/rawdata/{y}.csv"
    frame = pd.read_csv(path, header=None, names=cols, dtype=str, low_memory=False)
    frames.append(frame)
df = pd.concat(frames, ignore_index=True)
del frames

df = df[df["id"].str.startswith("US")]
df = df[df["element"].isin(["TMAX", "TMIN", "PRCP"])]
df = df[df["qflag"].isna() | (df["qflag"] == "")]

cnt = len(df)
t1 = time.time()
elapsed_ms = (t1 - t0) * 1000
print(f"Pandas | years={n_years} | rows={cnt} | time={elapsed_ms:.0f}ms")