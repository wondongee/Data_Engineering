from datetime import datetime, timedelta, timezone

KST = timezone(timedelta(hours=9))

def floor_to_2h(dt):
    dt = dt.astimezone(KST)
    floored_hour = (dt.hour // 2) * 2
    return dt.replace(hour=floored_hour, minute=0, second=0, microsecond=0)

def current_window_id(now=None):
    now = now or datetime.now(tz=KST)
    start = floor_to_2h(now)
    end = start + timedelta(hours=2)
    # ISO-8601(+09:00) 표기
    start_iso = start.isoformat(timespec="minutes")
    end_iso = end.isoformat(timespec="minutes")
    return f"{start_iso}..{end_iso}", start, end
