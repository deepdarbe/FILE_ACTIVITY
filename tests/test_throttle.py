"""Tests for the shared AttemptThrottle (#319 login + TOTP brute-force limiter)."""
from src.security.throttle import AttemptThrottle


def test_locks_after_max_failures():
    t = AttemptThrottle(max_attempts=3, window_s=300, lockout_s=900)
    allowed, ra = t.check("k")
    assert allowed is True and ra == 0
    for _ in range(3):
        t.record_failure("k")
    allowed, ra = t.check("k")
    assert allowed is False
    assert ra > 0  # a retry-after is reported


def test_under_threshold_not_locked():
    t = AttemptThrottle(max_attempts=5)
    for _ in range(4):  # one short of the lock
        t.record_failure("k")
    allowed, _ = t.check("k")
    assert allowed is True


def test_success_resets_counter():
    t = AttemptThrottle(max_attempts=3)
    for _ in range(2):
        t.record_failure("k")
    t.record_success("k")
    # Counter cleared → 2 more failures must not lock (needs 3 fresh).
    for _ in range(2):
        t.record_failure("k")
    allowed, _ = t.check("k")
    assert allowed is True


def test_keys_are_independent():
    t = AttemptThrottle(max_attempts=2)
    t.record_failure("a")
    t.record_failure("a")
    assert t.check("a")[0] is False   # 'a' locked
    assert t.check("b")[0] is True    # 'b' unaffected


def test_success_on_unknown_key_is_noop():
    t = AttemptThrottle()
    t.record_success("never-seen")  # must not raise
    assert t.check("never-seen")[0] is True
