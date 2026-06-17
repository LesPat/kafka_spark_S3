PROGRESS_LOG_INTERVAL_SECONDS = 30


def format_query_progress(query) -> str:
    progress = query.lastProgress
    if not progress:
        message = query.status.get("message", "waiting for first batch")
        return f"[{query.name}] {message}"

    duration = progress.get("durationMs", {}).get("triggerExecution", "n/a")
    return (
        f"[{query.name}] batch={progress.get('batchId')} "
        f"rows={progress.get('numInputRows', 0)} "
        f"input/s={progress.get('inputRowsPerSecond', 0):.2f} "
        f"duration_ms={duration}"
    )
