def format_duration(elapsed:float) -> str:
    mins,secs = divmod(elapsed, 60)
    hrs,mins = divmod(mins, 60)
    if hrs:
        return f"{int(hrs)}h {int(mins)}m {secs:.2f}s"
    if mins:
        return f"{int(hrs)}h {int(mins)}m {secs:.2f}s"
    return f"{secs:.2f}s"