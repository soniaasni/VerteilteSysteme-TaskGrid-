import time

MAX_WAIT_SECONDS = 60


def handle(payload: str) -> str:

    try:
        seconds = int(payload)

    except ValueError:
        raise ValueError(
            f"Ungültige Wartezeit: '{payload}'"
        )

    if seconds < 0:
        raise ValueError(
            "Wartezeit darf nicht negativ sein"
        )

    if seconds > MAX_WAIT_SECONDS:
        raise ValueError(
            f"Maximale Wartezeit überschritten "
            f"(max {MAX_WAIT_SECONDS}s)"
        )

    time.sleep(seconds)

    return f"waited {seconds}s"