# AI Disclosure

**Tool:** Claude Opus 5.5 (Anthropic), used through the chat interface.

## What I used it for

- **Code.** Templates and syntax for `spark_clean.py`, `ray_clean.py` and `compare_outputs.py`.
- **Debugging.** Help with Ray running out of memory (OOM) on my cluster. This led to freeing disk space, reducing the data from six to three months, and running Ray with a single shuffle aggregator.
- **Documentation.** Formatting and presenting the `.md` files 
- Understanding some of my outputs and results of the my runs.

## What I did myself

- Set up the VM, the Docker network and the three nodes, and installed everything.
- Downloaded the data and ran all Spark and Ray jobs.
- Collected all measurements (`runs.csv`, `spark_stats.csv`, `ray_stats.csv`) and all screenshots.
- Checked that both pipelines produce the same result with `compare_outputs.py` (`RESULT: IDENTICAL`).
- Modified the template code for actual purpose and wrote the report

All numbers in the report come from my own runs.
