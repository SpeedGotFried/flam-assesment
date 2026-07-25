"""
CLI Interface implementation for QueueCTL.
"""

import sys
import json
import argparse
from typing import List, Optional, Tuple
from queuectl_engine.db import Database
from queuectl_engine.config import ConfigManager
from queuectl_engine.worker import WorkerManager


def format_job_output(job: dict) -> dict:
    """Format job dictionary to match exact spec contract."""
    return {
        "id": job["id"],
        "command": job["command"],
        "state": job["state"],
        "attempts": job["attempts"],
        "max_retries": job["max_retries"],
        "created_at": job["created_at"],
        "updated_at": job["updated_at"]
    }


def handle_enqueue(args, db: Database):
    job_id = None
    command = None
    max_retries = getattr(args, "max_retries", None)

    # Check if raw JSON string passed as positional payload
    if getattr(args, "payload", None):
        payload_str = args.payload.strip()
        try:
            data = json.loads(payload_str)
            if isinstance(data, dict):
                job_id = data.get("id")
                command = data.get("command")
                if "max_retries" in data and max_retries is None:
                    max_retries = data.get("max_retries")
        except Exception:
            pass

    if not job_id and getattr(args, "id", None):
        job_id = args.id
    if not command and getattr(args, "command", None):
        command = args.command

    if not job_id or not command:
        print("Error: Job ID and command are required. Example: queuectl enqueue '{\"id\":\"job1\",\"command\":\"echo hi\"}'", file=sys.stderr)
        sys.exit(1)

    existing = db.get_job(job_id)
    if existing:
        print(f"Error: Job with ID '{job_id}' already exists.", file=sys.stderr)
        sys.exit(1)

    job = db.enqueue_job(job_id, command, max_retries=max_retries)
    print(f"Enqueued job '{job['id']}' [state: {job['state']}]")


def handle_worker(args, db: Database):
    subcommand = getattr(args, "worker_subcommand", None)
    if subcommand == "start":
        count = args.count
        print(f"Starting {count} worker(s) in foreground... (Press Ctrl+C or run 'queuectl worker stop' to exit)")
        manager = WorkerManager(db)
        manager.start_workers(count)
        print("Workers stopped gracefully.")
    elif subcommand == "stop":
        WorkerManager.stop_all_workers(db)
        print("Sent stop signal to active workers.")
    else:
        print("Error: Unknown worker subcommand. Use 'start' or 'stop'.", file=sys.stderr)
        sys.exit(1)


def handle_status(args, db: Database):
    counts = db.get_job_counts()
    active_workers = db.get_active_workers()

    print("=== QueueCTL Status ===")
    print(f"Active Workers : {len(active_workers)}")
    print("Job Breakdown:")
    for state in ["pending", "processing", "completed", "failed", "dead"]:
        print(f"  {state.capitalize():11}: {counts.get(state, 0)}")
    print("=======================")


def handle_list(args, db: Database):
    state = getattr(args, "state", None)
    is_json = getattr(args, "json", False)

    jobs = db.get_jobs_by_state(state)
    formatted_jobs = [format_job_output(j) for j in jobs]

    if is_json:
        # STRICT CONTRACT: Print ONLY valid JSON array to stdout, nothing else
        print(json.dumps(formatted_jobs, indent=2))
    else:
        if not formatted_jobs:
            print(f"No jobs found with state '{state}'" if state else "No jobs found.")
            return

        print(f"{'ID':<15} {'STATE':<12} {'ATTEMPTS':<10} {'MAX_RETRIES':<12} {'COMMAND'}")
        print("-" * 70)
        for j in formatted_jobs:
            print(f"{j['id']:<15} {j['state']:<12} {j['attempts']:<10} {j['max_retries']:<12} {j['command']}")


def handle_dlq(args, db: Database):
    subcommand = getattr(args, "dlq_subcommand", None)
    if subcommand == "list":
        jobs = db.get_jobs_by_state("dead")
        formatted_jobs = [format_job_output(j) for j in jobs]
        if getattr(args, "json", False):
            print(json.dumps(formatted_jobs, indent=2))
        else:
            if not formatted_jobs:
                print("DLQ is empty.")
                return
            print(f"{'ID':<15} {'ATTEMPTS':<10} {'CREATED_AT':<22} {'COMMAND'}")
            print("-" * 65)
            for j in formatted_jobs:
                print(f"{j['id']:<15} {j['attempts']:<10} {j['created_at']:<22} {j['command']}")

    elif subcommand == "retry":
        job_id = args.job_id
        if not job_id:
            print("Error: Job ID required for DLQ retry. Usage: queuectl dlq retry <job_id>", file=sys.stderr)
            sys.exit(1)
        
        success = db.re_enqueue_dlq_job(job_id)
        if success:
            print(f"Re-enqueued DLQ job '{job_id}' (state set to pending, attempts reset to 0).")
        else:
            job = db.get_job(job_id)
            if not job:
                print(f"Error: Job '{job_id}' not found.", file=sys.stderr)
            else:
                print(f"Error: Job '{job_id}' is in state '{job['state']}', not 'dead' (DLQ).", file=sys.stderr)
            sys.exit(1)


def handle_config(args, db: Database):
    cfg_mgr = ConfigManager(db)
    subcommand = getattr(args, "config_subcommand", None)

    if subcommand == "set":
        key = args.key
        val = args.value
        try:
            cfg_mgr.set(key, val)
            print(f"Config updated: {key} = {val}")
        except ValueError as exc:
            print(f"Error: {exc}", file=sys.stderr)
            sys.exit(1)

    elif subcommand == "get":
        key = getattr(args, "key", None)
        if key:
            val = cfg_mgr.get(key)
            print(f"{key} = {val}")
        else:
            configs = cfg_mgr.get_all()
            for k, v in configs.items():
                print(f"{k} = {v}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="queuectl",
        description="CLI-based background job queue system with worker retries and DLQ."
    )

    subparsers = parser.add_subparsers(dest="command", help="CLI commands")

    # enqueue
    p_enqueue = subparsers.add_parser("enqueue", help="Add a new job")
    p_enqueue.set_defaults(command="enqueue")
    p_enqueue.add_argument("payload", nargs="?", default=None, help="JSON string payload, e.g. '{\"id\":\"j1\",\"command\":\"sleep 2\"}'")
    p_enqueue.add_argument("--id", type=str, help="Job ID")
    p_enqueue.add_argument("--command", type=str, help="Job command")
    p_enqueue.add_argument("--max-retries", type=int, default=None, help="Max retries for this job")

    # worker
    p_worker = subparsers.add_parser("worker", help="Worker management")
    p_worker.set_defaults(command="worker")
    w_sub = p_worker.add_subparsers(dest="worker_subcommand")
    w_start = w_sub.add_parser("start", help="Start worker processes in foreground")
    w_start.set_defaults(command="worker", worker_subcommand="start")
    w_start.add_argument("--count", type=int, default=1, help="Number of worker threads/processes")
    w_stop = w_sub.add_parser("stop", help="Stop all workers gracefully")
    w_stop.set_defaults(command="worker", worker_subcommand="stop")

    # status
    p_status = subparsers.add_parser("status", help="Summary of job states and active workers")
    p_status.set_defaults(command="status")

    # list
    p_list = subparsers.add_parser("list", help="List jobs by state")
    p_list.set_defaults(command="list")
    p_list.add_argument("--state", type=str, choices=["pending", "processing", "completed", "failed", "dead"], help="Filter by state")
    p_list.add_argument("--json", action="store_true", help="Output JSON array only")

    # dlq
    p_dlq = subparsers.add_parser("dlq", help="Dead Letter Queue management")
    p_dlq.set_defaults(command="dlq")
    d_sub = p_dlq.add_subparsers(dest="dlq_subcommand")
    d_list = d_sub.add_parser("list", help="List dead jobs in DLQ")
    d_list.set_defaults(command="dlq", dlq_subcommand="list")
    d_list.add_argument("--json", action="store_true", help="Output JSON array only")
    d_retry = d_sub.add_parser("retry", help="Re-enqueue a dead job")
    d_retry.set_defaults(command="dlq", dlq_subcommand="retry")
    d_retry.add_argument("job_id", type=str, help="Job ID to retry")

    # config
    p_config = subparsers.add_parser("config", help="Manage configuration")
    p_config.set_defaults(command="config")
    c_sub = p_config.add_subparsers(dest="config_subcommand")
    c_set = c_sub.add_parser("set", help="Set a configuration key")
    c_set.set_defaults(command="config", config_subcommand="set")
    c_set.add_argument("key", type=str, help="Config key")
    c_set.add_argument("value", type=str, help="Config value")
    c_get = c_sub.add_parser("get", help="Get configuration value(s)")
    c_get.set_defaults(command="config", config_subcommand="get")
    c_get.add_argument("key", nargs="?", default=None, type=str, help="Config key (optional)")

    return parser


def extract_db_arg(cli_args: Optional[List[str]]) -> Tuple[Optional[str], List[str]]:
    if cli_args is None:
        cli_args = sys.argv[1:]
    
    db_path = None
    clean_args = []
    i = 0
    while i < len(cli_args):
        arg = cli_args[i]
        if arg == "--db" and i + 1 < len(cli_args):
            db_path = cli_args[i + 1]
            i += 2
        elif arg.startswith("--db="):
            db_path = arg.split("=", 1)[1]
            i += 1
        else:
            clean_args.append(arg)
            i += 1
    return db_path, clean_args


def main(cli_args: Optional[List[str]] = None):
    db_path, clean_args = extract_db_arg(cli_args)
    parser = build_parser()
    args = parser.parse_args(clean_args)

    if not getattr(args, "command", None):
        parser.print_help()
        sys.exit(0)

    db = Database(db_path)

    if args.command == "enqueue":
        handle_enqueue(args, db)
    elif args.command == "worker":
        handle_worker(args, db)
    elif args.command == "status":
        handle_status(args, db)
    elif args.command == "list":
        handle_list(args, db)
    elif args.command == "dlq":
        handle_dlq(args, db)
    elif args.command == "config":
        handle_config(args, db)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
