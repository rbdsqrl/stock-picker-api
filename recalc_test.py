from database import recalculate_all_levels, check_and_update_target_hits
import sqlite3

print("Running recalculate_all_levels()...", flush=True)
n = recalculate_all_levels()
print(f"Recalculated: {n}", flush=True)

h = check_and_update_target_hits()
print(f"Target hits updated: {h}", flush=True)

conn = sqlite3.connect("../data/stockpick.db")
conn.row_factory = sqlite3.Row
for r in conn.execute("SELECT ticker, date, rank, stop_pct, target_pct, rr_ratio, target_hit FROM picks ORDER BY date DESC, rank"):
    hit = "HIT" if r["target_hit"] == 1 else ("MISS" if r["target_hit"] == 0 else "pending")
    print(f"  {r['ticker']:12s} {r['date']} #{r['rank']}  stop=-{r['stop_pct']}%  target=+{r['target_pct']}%  rr=1:{r['rr_ratio']}  {hit}")
conn.close()
print("DONE", flush=True)
