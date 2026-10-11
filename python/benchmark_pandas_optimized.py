import sys, time
import pandas as pd

n_years = int(sys.argv[1]) if len(sys.argv) > 1 else 1
years = list(range(2000, 2000 + n_years))
cols = ["id", "date", "element", "value", "mflag", "qflag", "sflag", "obstime"]

# Efficient dtypes an experienced pandas user would actually choose —
# category for low-cardinality strings, proper numeric type for value,
# skip parsing columns we don't need at all.
dtypes = {
    "id": "category",
    "date": "str",
    "element": "category",
    "value": "float32",
    "mflag": "category",
    "qflag": "category",
    "sflag": "category",
}

t0 = time.time()
frames = []
for y in years:
    path = f"/rawdata/{y}.csv"
    frame = pd.read_csv(
        path, header=None, skiprows=1, names=cols, dtype=dtypes,
        usecols=["id", "date", "element", "value", "qflag"],  # skip mflag/sflag/obstime entirely
    )
    frame = frame[frame["id"].str.startswith("US")]
    frame = frame[frame["element"].isin(["TMAX", "TMIN", "PRCP"])]
    frame = frame[frame["qflag"].isna()]
    frames.append(frame)

df = pd.concat(frames, ignore_index=True)
cnt = len(df)
t1 = time.time()
elapsed_ms = (t1 - t0) * 1000
print(f"Pandas(optimized) | years={n_years} | rows={cnt} | time={elapsed_ms:.0f}ms")