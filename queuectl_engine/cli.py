"""
Strict Minimal CLI Interface for QueueCTL.
"""

import sys
import json
import argparse
from typing import List, Optional, Tuple
from queuectl_engine.db import Database
from queuectl_engine.config import ConfigManager
from queuectl_engine.worker import WorkerManager


def format_job_output(job: dict) -> dict:
    return {
        "id": job["id"],
        "command": job["command"],
        "state": job["state"],
        "attempts": job["attempts"],
        "max_retries": job["max_retries"],
        "created_at": job["created_at"],
        "updated_at": job["updated_at"],
    }


def handle_enqueue(args, db: Database):
    if not args.payload:
        print("Error: Job JSON payload required. Example: queuectl enqueue '{\"id\":\"job1\",\"command\":\"sleep 2\"}'", file=sys.stderr)
        sys.exit(1)

    try:
        data = json.loads(args.payload.strip())
        if not isinstance(data, dict):
            raise ValueError("Payload must be a JSON object")
    except Exception as e:
        print(f"Error: Invalid JSON payload: {e}", file=sys.stderr)
        sys.exit(1)

    job_id = data.get("id")
    command = data.get("command")
    max_retries = data.get("max_retries")

    if not job_id or not command:
        print("Error: Job 'id' and 'command' are required in JSON.", file=sys.stderr)
        sys.exit(1)

    if db.get_job(job_id):
        print(f"Error: Job '{job_id}' already exists.", file=sys.stderr)
        sys.exit(1)

    job = db.enqueue_job(job_id, command, max_retries=max_retries)
    print(f"Enqueued job '{job['id']}' [state: {job['state']}]")


def handle_worker(args, db: Database):
    sub = getattr(args, "worker_subcommand", None)
    if sub == "start":
        print(f"Starting {args.count} worker(s)...")
        WorkerManager(db).start_workers(args.count)
        print("Workers stopped.")
    elif sub == "stop":
        WorkerManager.stop_all_workers(db)
        print("Sent stop signal to workers.")
    else:
        print("Error: Unknown worker subcommand.", file=sys.stderr)
        sys.exit(1)


def handle_status(args, db: Database):
    counts = db.get_job_counts()
    active = db.get_active_workers()
    print("=== QueueCTL Status ===")
    print(f"Active Workers : {len(active)}")
    print("Job Breakdown:")
    for state in ["pending", "processing", "completed", "failed", "dead"]:
        print(f"  {state.capitalize():11}: {counts.get(state, 0)}")
    print("=======================")


def handle_list(args, db: Database):
    jobs = db.get_jobs_by_state(getattr(args, "state", None))
    fmt = [format_job_output(j) for j in jobs]
    print(json.dumps(fmt, indent=2))


def handle_dlq(args, db: Database):
    sub = getattr(args, "dlq_subcommand", None)
    if sub == "list":
        jobs = db.get_jobs_by_state("dead")
        fmt = [format_job_output(j) for j in jobs]
        print(json.dumps(fmt, indent=2))
    elif sub == "retry":
        if not getattr(args, "job_id", None):
            print("Error: Job ID required.", file=sys.stderr)
            sys.exit(1)
        if db.re_enqueue_dlq_job(args.job_id):
            print(f"Re-enqueued DLQ job '{args.job_id}'.")
        else:
            print(f"Error: Job '{args.job_id}' not in DLQ.", file=sys.stderr)
            sys.exit(1)


def handle_config(args, db: Database):
    cfg = ConfigManager(db)
    sub = getattr(args, "config_subcommand", None)
    if sub == "set":
        cfg.set(args.key, args.value)
        print(f"Config updated: {args.key} = {args.value}")
    elif sub == "get":
        if args.key:
            print(f"{args.key} = {cfg.get(args.key)}")
        else:
            for k, v in cfg.get_all().items():
                print(f"{k} = {v}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="queuectl", description="QueueCTL CLI.")
    subp = parser.add_subparsers(dest="command")

    p_enq = subp.add_parser("enqueue")
    p_enq.set_defaults(command="enqueue")
    p_enq.add_argument("payload", nargs="?", default=None)

    p_wrk = subp.add_parser("worker")
    p_wrk.set_defaults(command="worker")
    w_sub = p_wrk.add_subparsers(dest="worker_subcommand")
    w_st = w_sub.add_parser("start")
    w_st.set_defaults(command="worker", worker_subcommand="start")
    w_st.add_argument("--count", type=int, default=1)
    w_sp = w_sub.add_parser("stop")
    w_sp.set_defaults(command="worker", worker_subcommand="stop")

    p_stat = subp.add_parser("status")
    p_stat.set_defaults(command="status")

    p_lst = subp.add_parser("list")
    p_lst.set_defaults(command="list")
    p_lst.add_argument("--state", type=str, choices=["pending", "processing", "completed", "failed", "dead"])
    p_lst.add_argument("--json", action="store_true")

    p_dlq = subp.add_parser("dlq")
    p_dlq.set_defaults(command="dlq")
    d_sub = p_dlq.add_subparsers(dest="dlq_subcommand")
    d_lst = d_sub.add_parser("list")
    d_lst.set_defaults(command="dlq", dlq_subcommand="list")
    d_lst.add_argument("--json", action="store_true")
    d_ret = d_sub.add_parser("retry")
    d_ret.set_defaults(command="dlq", dlq_subcommand="retry")
    d_ret.add_argument("job_id", type=str)

    p_cfg = subp.add_parser("config")
    p_cfg.set_defaults(command="config")
    c_sub = p_cfg.add_subparsers(dest="config_subcommand")
    c_st = c_sub.add_parser("set")
    c_st.set_defaults(command="config", config_subcommand="set")
    c_st.add_argument("key", type=str)
    c_st.add_argument("value", type=str)
    c_gt = c_sub.add_parser("get")
    c_gt.set_defaults(command="config", config_subcommand="get")
    c_gt.add_argument("key", nargs="?", default=None, type=str)

    return parser


def extract_db_arg(cli_args: Optional[List[str]]) -> Tuple[Optional[str], List[str]]:
    if cli_args is None:
        cli_args = sys.argv[1:]
    db_path, clean_args, i = None, [], 0
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
    handlers = {
        "enqueue": handle_enqueue,
        "worker": handle_worker,
        "status": handle_status,
        "list": handle_list,
        "dlq": handle_dlq,
        "config": handle_config,
    }
    handler = handlers.get(args.command)
    if handler:
        handler(args, db)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
