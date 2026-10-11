# Legacy scripts (kept for history, not used)

Early Part 01–05 helpers, superseded by the current workflow in `PROJECT_GUIDE.md` / `docs/RESUME.md`:

| File | Why it is no longer used |
|---|---|
| `01_start_cluster.sh` … `04_verify_output.sh` | bash versions of the first steps; they use the old container names (`namenode`, `spark`) from before the containers were renamed `flu-*`. Use `docker compose up -d`, `scripts/verify_cluster.ps1`, `scripts/run_scala.ps1`. |
| `drip.sh` | first streaming drip loop (Windows line endings, never worked); replaced by `streaming/drip_feed.sh` + `benchmarks/drip_streaming_v2.ps1`. |
| `verify_snippet.scala` | 4-line ad-hoc ILINet check; replaced by `scala/ClusterSmokeTest.scala` (all tables, JSON evidence). |
