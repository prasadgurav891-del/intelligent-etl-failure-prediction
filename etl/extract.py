import gzip
import json
import time


def iter_runs(path, limit=None, on_error=None, before=None, on_parsed=None):
    """Yield one parsed JSONL run at a time; malformed lines are reported and skipped."""
    opener = gzip.open if str(path).lower().endswith(".gz") else open
    yielded = 0
    with opener(path, "rt", encoding="utf-8") as stream:
        for line_no, line in enumerate(stream, 1):
            if before:
                before(line_no, line)
            try:
                parse_started = time.perf_counter()
                item = json.loads(line)
                if not isinstance(item, dict):
                    raise ValueError("JSON record must be an object")
                if on_parsed:
                    on_parsed(line_no, time.perf_counter() - parse_started)
                yield item
                yielded += 1
                if limit is not None and yielded >= limit:
                    return
            except (json.JSONDecodeError, ValueError) as exc:
                if on_error:
                    on_error(line_no, exc)

