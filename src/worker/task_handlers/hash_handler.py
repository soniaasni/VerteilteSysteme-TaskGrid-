import hashlib


def handle(payload: str) -> str:
    if payload is None:
        raise ValueError("Payload für hash darf nicht None sein")

    return hashlib.sha256(payload.encode("utf-8")).hexdigest()