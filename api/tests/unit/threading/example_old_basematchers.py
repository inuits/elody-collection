import threading


class BrokenBaseMatchers:
    collection = "default"


def test_broken_implementation_fails_isolation():
    """
    This test asserts that BaseMatchers IS thread-safe.
    Because BrokenBaseMatchers is NOT thread-safe, this test will FAIL.
    This mimincs the old BaseMatchers implementation from for example
    this time:
    https://gitlab.inuits.io/rnd/inuits/dams/inuits-dams-collection/-/blob/da13dd7baf1bd208320577e38fd27fcbd815ab20/api/filters_v2/matchers/base_matchers.py
    """
    # Barrier to force threads to sync up perfectly
    barrier = threading.Barrier(2)

    results = {"t1_value": None, "t2_value": None}

    def worker_1():
        # Thread 1 sets the value to "entities"
        BrokenBaseMatchers.collection = "entities"

        # WAIT for Thread 2 to overwrite it with "mediafiles"
        barrier.wait()

        # Read the value back (Expecting "entities", but will get "mediafiles")
        results["t1_value"] = BrokenBaseMatchers.collection

    def worker_2():
        # Thread 2 sets the value to "mediafiles"
        BrokenBaseMatchers.collection = "mediafiles"

        # WAIT for Thread 1 to be ready
        barrier.wait()

        # Read the value back
        results["t2_value"] = BrokenBaseMatchers.collection

    t1 = threading.Thread(target=worker_1)
    t2 = threading.Thread(target=worker_2)

    t1.start()
    t2.start()

    t1.join()
    t2.join()

    # --- ASSERTIONS ---

    # Thread 2 is usually fine because it ran last
    assert results["t2_value"] == "mediafiles"

    assert results["t1_value"] == "entities"
