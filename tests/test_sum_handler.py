from src.worker.task_handlers.sum_handler import handle


def test_sum_multiple_numbers():
    assert handle("1,2,3,4") == "10"


def test_sum_two_numbers():
    assert handle("10,20") == "30"


def test_sum_single_number():
    assert handle("0") == "0"


def test_sum_negative_numbers():
    assert handle("-5,10") == "5"


def test_sum_invalid_input():
    try:
        handle("1,2,a,4")
        assert False, "Es hätte eine Exception geworfen werden müssen"
    except ValueError:
        pass


def test_sum_empty_string():
    try:
        handle("")
        assert False, "Es hätte eine Exception geworfen werden müssen"
    except ValueError:
        pass