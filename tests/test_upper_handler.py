from src.worker.task_handlers.upper import handle


def test_upper_hello_world():
    assert handle("hello world") == "HELLO WORLD"


def test_upper_with_numbers():
    assert handle("abc123") == "ABC123"


def test_upper_empty_string():
    assert handle("") == ""


def test_upper_unicode():
    assert handle("äöü") == "ÄÖÜ"


def test_upper_none():
    try:
        handle(None)
        assert False, "Es hätte eine Exception geworfen werden müssen"
    except ValueError:
        pass