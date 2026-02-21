"""Export dataset to JSON for React frontend."""
import pandas as pd
import json
import os

df = pd.read_csv("data/processed/missioni_complete.csv")

out_dir = "frontend/public/data"
os.makedirs(out_dir, exist_ok=True)

# missions.json — full dataset
missions = df.to_dict(orient="records")
with open(f"{out_dir}/missions.json", "w", encoding="utf-8") as f:
    json.dump(missions, f, default=str, ensure_ascii=False)

# active.json — active only
active = df[df["is_active"] == True].to_dict(orient="records")
with open(f"{out_dir}/active.json", "w", encoding="utf-8") as f:
    json.dump(active, f, default=str, ensure_ascii=False)

# stats.json — pre-computed KPIs
by_decade = {}
for _, r in df.iterrows():
    if pd.notna(r["data_inizio"]):
        try:
            decade = str(int(str(r["data_inizio"])[:4]) // 10 * 10)
            by_decade[decade] = by_decade.get(decade, 0) + 1
        except (ValueError, TypeError):
            pass

active_df = df[df["is_active"] == True]
stats = {
    "total": int(len(df)),
    "active": int(len(active_df)),
    "personnel": int(active_df["personale_totale"].sum()),
    "countries": int(active_df["paese"].nunique()),
    "organizations": int(df["tipo_missione"].nunique()),
    "regions": int(df["regione"].nunique()),
    "by_org": active_df.groupby("tipo_missione").size().to_dict(),
    "by_region": active_df.groupby("regione").size().to_dict(),
    "by_decade": by_decade,
}
with open(f"{out_dir}/stats.json", "w", encoding="utf-8") as f:
    json.dump(stats, f, ensure_ascii=False)

print(f"Exported {len(missions)} missions, {len(active)} active, stats to {out_dir}/")
