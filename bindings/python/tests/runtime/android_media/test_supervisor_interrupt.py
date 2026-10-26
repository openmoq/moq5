"""An operator interruption (KeyboardInterrupt: SIGINT, or the live entry's converted SIGTERM) arriving
while the real supervisor runs, retires its probe threads, or settles its children. The run must be
recorded as interrupted, every owned child settled -- the one being settled when the interruption
arrived included -- or named as retained, and the supervisor's record returned, never a passing verdict.

Every run is offline, under the real `supervise()` with the route rig's owned stub children. The
interruptions are injected at named points on the supervisor's own thread, never by signal timing. The
fixture holds its own handle to every child the supervisor spawns and settles whatever is left, so an
intentional RED cannot leak a child, a reader or a descriptor.
"""
import signal
import sys
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent))

import run_android_media as m                                # noqa: E402
from test_supervisor_route import ONE_SUB, TWO_SUBS, Run     # noqa: E402


class Owned:
    """The supervisor's child classes, replaced by subclasses that also register each child here and
    route every settle through `plan`: {child name: how many settle calls raise KeyboardInterrupt
    before delegating}. The fixture's own cleanup settles every registered child still unsettled."""

    def __init__(self, test, plan=None, errors=None):
        self.children, self.settle_calls, self.plan = [], [], dict(plan or {})
        self.errors = dict(errors or {})                  # {child name: an exception its first settle raises}
        owner = self
        real_settle = m.Child.settle

        def register(cls):
            class Registered(cls):
                def __init__(self, *a, **kw):
                    super().__init__(*a, **kw)
                    owner.children.append(self)

                def settle(self, *a, **kw):
                    owner.settle_calls.append(self.name)
                    if owner.plan.get(self.name, 0) > 0:
                        owner.plan[self.name] -= 1
                        raise KeyboardInterrupt(f"injected while settling {self.name}")
                    if self.name in owner.errors:
                        raise owner.errors.pop(self.name)
                    return real_settle(self, *a, **kw)
            return Registered
        saved = (m.Child, m.RawStdoutChild, m.DiagnosticChild)
        m.Child, m.RawStdoutChild, m.DiagnosticChild = (register(c) for c in saved)
        test.addCleanup(self._retire, real_settle)
        test.addCleanup(setattr, m, "DiagnosticChild", saved[2])
        test.addCleanup(setattr, m, "RawStdoutChild", saved[1])
        test.addCleanup(setattr, m, "Child", saved[0])

    def _retire(self, real_settle):
        for c in reversed(self.children):
            if c.settled is None:
                real_settle(c, 3.0, signal_first=signal.SIGTERM)

    def alive(self):
        return [(c.name, c.proc.pid) for c in self.children if c.proc.poll() is None]


def interrupt_once(where):
    fired = []

    def hook(*_):
        if not fired:
            fired.append(where)
            raise KeyboardInterrupt(f"injected during {where}")
    return hook


class SupervisorInterruption(unittest.TestCase):
    def supervise(self, plan=None, hooks=None, errors=None):
        self.owned = Owned(self, plan, errors)
        run = Run(self, [ONE_SUB, ONE_SUB, TWO_SUBS])
        run.actors.hooks.update(hooks or {})
        try:
            rec = run.supervise()
        except KeyboardInterrupt as e:
            self.fail(f"the interruption escaped the supervisor: {e!r}; still alive {self.owned.alive()}")
        return rec

    def assert_interrupted_and_retired(self, rec, where):
        self.assertTrue((rec["primary_error"] or "").startswith(f"interrupted during {where}"), rec["primary_error"])
        self.assertEqual([i["where"] for i in rec["interruptions"]][:1], [where])
        self.assertFalse(rec["verdict"]["ok"])
        self.assertIn(f"interrupted during {where}", rec["verdict"]["failures"])
        self.assertEqual(rec["retained"], [])
        self.assertEqual(sorted(c["name"] for c in rec["children"]), ["host-receiver", "player-log", "publisher", "relay"])
        for c in rec["children"]:
            self.assertIsNotNone(c.get("exit"), c)
            self.assertIs(c.get("readers_settled"), True, c)
        self.assertEqual(self.owned.alive(), [])                        # reaped by the supervisor, not by the fixture
        self.assertIs(rec["threads_settled"], True)

    def test_an_interruption_during_supervision_is_recorded_and_every_child_retired(self):
        rec = self.supervise(hooks={"forwarded": lambda control: control == "ack-all" and interrupt_once("supervision")()})
        self.assert_interrupted_and_retired(rec, "supervision")

    def test_an_interruption_while_retiring_a_probe_thread_still_joins_it(self):
        joins, interrupt = [], interrupt_once("probe-thread retirement")

        def joining(thread):
            joins.append(thread)
            interrupt()
        rec = self.supervise(hooks={"joining": joining})
        self.assert_interrupted_and_retired(rec, "probe-thread retirement")
        self.assertIs(joins[1], joins[0])                               # the interrupted join is attempted again
        self.assertEqual(len(joins), len(set(map(id, joins))) + 1)      # every thread joined; only that one twice

    def test_an_interruption_while_settling_a_child_completes_that_child_and_the_rest(self):
        rec = self.supervise(plan={"host-receiver": 1})
        self.assert_interrupted_and_retired(rec, "settling host-receiver")
        self.assertEqual(self.owned.settle_calls[:2], ["host-receiver", "host-receiver"])   # the current child again
        self.assertEqual(sorted(set(self.owned.settle_calls)), ["host-receiver", "player-log", "publisher", "relay"])

    def test_a_child_whose_settlement_is_interrupted_twice_is_named_retained(self):
        rec = self.supervise(plan={"relay": 2})
        relay = next(c for c in self.owned.children if c.name == "relay")
        self.assertEqual(rec["retained"], [{"name": "relay", "pid": relay.proc.pid,
                                            "why": "settlement interrupted twice; not proven reaped"}])
        self.assertIn(f"retained: relay pid {relay.proc.pid}: settlement interrupted twice; not proven reaped",
                      rec["verdict"]["failures"])
        record = next(c for c in rec["children"] if c["name"] == "relay")
        self.assertIs(record.get("settled"), False)
        self.assertIsNone(record.get("exit"))                          # never claimed reaped
        others = [c for c in rec["children"] if c["name"] != "relay"]
        self.assertTrue(all(c.get("exit") is not None and c.get("readers_settled") for c in others), others)


    def test_a_settle_that_fails_is_named_retained_and_the_rest_still_settle(self):
        rec = self.supervise(errors={"publisher": OSError("settle refused")})
        publisher = next(c for c in self.owned.children if c.name == "publisher")
        self.assertEqual(rec["retained"], [{"name": "publisher", "pid": publisher.proc.pid,
                                            "why": "settle raised OSError('settle refused'); not proven reaped"}])
        self.assertFalse(rec["verdict"]["ok"])
        others = [c for c in rec["children"] if c["name"] != "publisher"]
        self.assertTrue(all(c.get("exit") is not None and c.get("readers_settled") for c in others), others)


if __name__ == "__main__":
    unittest.main()
