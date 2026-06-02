def handle(payload: str) -> str:
    if payload is None:
        raise ValueError("Payload für reverse darf nicht None sein")

    return payload[::-1]