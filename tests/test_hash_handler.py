from src.worker.task_handlers.hash_handler import handle


def test_hash_hello():
    assert (
        handle("hello")
        == "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
    )


def test_hash_empty_string():
    assert (
        handle("")
        == "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    )


def test_hash_unicode():
    assert handle("äöü") == hashlib_expected("äöü")


def test_hash_none():
    try:
        handle(None)
        assert False, "Es hätte eine Exception geworfen werden müssen"
    except ValueError:
        pass


def hashlib_expected(value: str) -> str:
    import hashlib
    return hashlib.sha256(value.encode("utf-8")).hexdigest()