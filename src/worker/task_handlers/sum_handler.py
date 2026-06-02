def handle(payload: str) -> str:

    try:
        numbers = [
            int(number.strip())
            for number in payload.split(",")
        ]

    except ValueError:
        raise ValueError(
            f"Ungültige Zahlenliste: '{payload}'"
        )

    return str(sum(numbers))