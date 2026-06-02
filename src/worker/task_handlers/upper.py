def handle(payload: str) -> str:
    if payload is None:
        raise ValueError("Payload für upper darf nicht None sein")

    return payload.upper()