from src.worker.task_handlers.reverse import handle


def test_reverse_hello():
    assert handle("hello") == "olleh"


def test_reverse_abcde():
    assert handle("abcde") == "edcba"


def test_reverse_empty_string():
    assert handle("") == ""


def test_reverse_unicode():
    assert handle("äöü") == "üöä"


def test_reverse_special_characters():
    assert handle("!@#$") == "$#@!"


def test_reverse_emoji():
    assert handle("abc😊") == "😊cba"


def test_reverse_none():
    try:
        handle(None)
        assert False, "Es hätte eine Exception geworfen werden müssen"
    except ValueError:
        pass