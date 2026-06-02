import time

from src.worker.task_handlers.wait_handler import handle


def test_wait_one_second():

    start = time.time()

    result = handle("1")

    duration = time.time() - start

    assert result == "waited 1s"
    assert duration >= 1


def test_wait_zero_seconds():
    assert handle("0") == "waited 0s"


def test_wait_negative_seconds():

    try:
        handle("-1")
        assert False

    except ValueError:
        pass


def test_wait_invalid_payload():

    try:
        handle("abc")
        assert False

    except ValueError:
        pass


def test_wait_too_large():

    try:
        handle("100")
        assert False

    except ValueError:
        pass