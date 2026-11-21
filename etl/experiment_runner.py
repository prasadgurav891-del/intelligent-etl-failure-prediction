import argparse
import random

import psycopg2

from etl.config import Config
from etl.pipeline import run_pipeline

SCENARIOS = ("NORMAL", "LARGE_DATA", "INVALID_DATA", "TRANSFORMATION_FAILURE",
             "DATABASE_FAILURE_SIMULATION", "RESOURCE_STRESS", "RESOURCE_THRESHOLD_FAILURE")


def summarize(config=None):
    config = config or Config.from_env()
    with psycopg2.connect(**config.db_kwargs()) as conn, conn.cursor() as cur:
        cur.execute("SELECT overall_status,count(*) FROM etl_executions GROUP BY overall_status")
        executions = dict(cur.fetchall())
        cur.execute("""SELECT count(*) FILTER (WHERE status IN ('SUCCESS','FAILURE')),
          count(*) FILTER (WHERE status='SUCCESS'),count(*) FILTER (WHERE status='FAILURE'),
          count(*) FILTER (WHERE status='SKIPPED'),
          avg(duration_sec) FILTER (WHERE status IN ('SUCCESS','FAILURE')),
          avg(cpu_percent) FILTER (WHERE status IN ('SUCCESS','FAILURE')),
          avg(memory_percent) FILTER (WHERE status IN ('SUCCESS','FAILURE')),
          avg(throughput) FILTER (WHERE status IN ('SUCCESS','FAILURE')) FROM etl_task_metadata""")
        total, success, failed, skipped, duration, cpu, memory, throughput = cur.fetchone()
        cur.execute("SELECT scenario,count(*) FROM etl_executions GROUP BY scenario ORDER BY scenario")
        scenarios = cur.fetchall()
    print(f"Total ETL executions: {sum(executions.values())}; successful: {executions.get('SUCCESS',0)}; failed: {executions.get('FAILURE',0)}")
    if total:
        print(f"Executed task executions: {total}; successful tasks: {success}; failed tasks: {failed}; skipped tasks: {skipped}; failure rate: {failed/total:.2%}")
    else:
        print("No task executions")
    print(f"Average task duration: {duration or 0:.3f}s; CPU: {cpu or 0:.2f}%; memory: {memory or 0:.2f}%; throughput: {throughput or 0:.2f} records/s")
    print("Counts by scenario:")
    for scenario, count in scenarios:
        print(f"{scenario}: {count}")


def main():
    parser = argparse.ArgumentParser(description="Repeat ETL runs under reproducible scenarios")
    parser.add_argument("--runs", type=int, default=1)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--limit", type=int, help="Optional cap for each run")
    parser.add_argument("--scenario", choices=SCENARIOS, help="Pin all iterations to one scenario")
    args = parser.parse_args()
    if args.runs < 1:
        parser.error("--runs must be at least 1")
    rng = random.Random(args.seed)
    config = Config.from_env()
    for i in range(args.runs):
        scenario = args.scenario or rng.choice(SCENARIOS)
        print(f"Experiment {i+1}/{args.runs}: {scenario}")
        try:
            run_pipeline(config, limit=args.limit, scenario=scenario, seed=args.seed)
        except Exception as exc:
            print(f"Experiment failed and will continue: {type(exc).__name__}: {exc}")
    summarize(config)


if __name__ == "__main__":
    main()
