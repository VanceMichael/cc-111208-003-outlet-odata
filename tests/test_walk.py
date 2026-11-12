"""End-to-end regression tests for the walk business flow.

These tests exercise the real pieces together: a local replay server serves
the synthetic OData catalogue, the walker fetches it over loopback HTTP, and
every completeness decision is read back from the SQLite ledger.  The crash
test starts a real subprocess and kills it, per docs/acceptance.md.
"""

import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import threading
import unittest

from outletwalk.ledger import Ledger
from outletwalk.walker import Walker
from scripts.materialize import main as materialize
from scripts.source_server import ReplayServer, running

FIXTURES = str(Path(__file__).resolve().parents[1] / 'fixtures')


def server(scenario):
    return running(root=FIXTURES, scenario=scenario)


def walk_once(state, origin, max_pages=1000, concurrency=2, path='/outlets'):
    walker = Walker(state, origin + path, origin, max_pages=max_pages,
                    concurrency=concurrency)
    try:
        return walker.run()
    finally:
        walker.close()


def ledger_view(state):
    """Open a read-only ledger handle; caller closes it via close()."""
    return Ledger(state)


class WalkFlowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        materialize()

    # ---------------- two-phase demo shape: paused then complete ----------------
    def test_root_ending_is_not_walk_completion_and_resume_finishes(self):
        with server('walk.json') as srv:
            origin = srv.origin
            with tempfile.TemporaryDirectory() as state:
                # Budget covers the two root pages but not A's child page.
                first = walk_once(state, origin, max_pages=2, concurrency=1)
                self.assertEqual(first['status'], 'paused')
                self.assertIn('budget', first['reason'])
                root_chain = next(c for c in first['chains'] if c['kind'] == 'root')
                self.assertEqual(root_chain['state'], 'complete')
                child = next(c for c in first['chains'] if c['outlet_id'] == 'A')
                self.assertEqual(child['state'], 'open')
                self.assertEqual(child['url'], origin + '/slots/A?cursor=x')
                # B already has its full (inline-only) collection and is queryable.
                view = ledger_view(state)
                try:
                    b = view.query_outlet('B')
                    self.assertEqual(b['state'], 'complete')
                    self.assertEqual([slot['slot_id'] for slot in b['slots']], ['1'])
                    # A must not look complete yet, and must say where it is stuck.
                    a = view.query_outlet('A')
                    self.assertEqual(a['state'], 'open')
                    self.assertEqual(a['child_chain']['url'], origin + '/slots/A?cursor=x')
                finally:
                    view.close()
                # Restart in a brand new process state: resume from the ledger.
                second = walk_once(state, origin, max_pages=10, concurrency=1)
                self.assertEqual(second['status'], 'complete')
                self.assertIsNone(second['reason'])
                ledger = Ledger(state)
                try:
                    self.assertTrue(all(c['state'] == 'complete' for c in second['chains']))
                    a = ledger.query_outlet('A')
                    self.assertEqual(a['state'], 'complete')
                    self.assertEqual([slot['slot_id'] for slot in a['slots']], ['1', '2'])
                finally:
                    ledger.close()

    def test_two_steps_are_distinguishable_in_persisted_status(self):
        with server('walk.json') as srv:
            with tempfile.TemporaryDirectory() as state:
                walk_once(state, srv.origin, max_pages=2, concurrency=1)
                view = ledger_view(state)
                try:
                    paused = view.snapshot()
                finally:
                    view.close()
                self.assertEqual(paused['status'], 'paused')
                walk_once(state, srv.origin, max_pages=10, concurrency=1)
                view = ledger_view(state)
                try:
                    done = view.snapshot()
                finally:
                    view.close()
                self.assertEqual(done['status'], 'complete')
                self.assertGreater(done['budget_used'], paused['budget_used'])

    # ---------------- identity: parent scoping, duplicate slot ids ----------------
    def test_same_slot_id_under_different_parents_is_not_mixed(self):
        with server('walk.json') as srv:
            with tempfile.TemporaryDirectory() as state:
                walk_once(state, srv.origin, concurrency=2)
                ledger = Ledger(state)
                try:
                    a, b = ledger.query_outlet('A'), ledger.query_outlet('B')
                    self.assertEqual([s['slot_id'] for s in a['slots']], ['1', '2'])
                    self.assertEqual([s['slot_id'] for s in b['slots']], ['1'])
                finally:
                    ledger.close()
                # Storage keeps parent identity on every row.
                with sqlite3.connect(state + '/walk.sqlite3') as db:
                    rows = db.execute(
                        'SELECT outlet_id, slot_id FROM slots ORDER BY outlet_id, slot_id').fetchall()
                self.assertEqual(rows, [('A', '1'), ('A', '2'), ('B', '1')])

    def test_duplicate_slot_on_child_page_is_stored_once(self):
        with server('walk.json') as srv:
            with tempfile.TemporaryDirectory() as state:
                walk_once(state, srv.origin, concurrency=1)
                view = ledger_view(state)
                try:
                    slots = view.query_outlet('A')['slots']
                finally:
                    view.close()
                self.assertEqual([s['slot_id'] for s in slots], ['1', '2'])

    def test_same_outlet_repeated_on_root_pages_does_not_duplicate(self):
        with server('walk_repeat.json') as srv:
            with tempfile.TemporaryDirectory() as state:
                result = walk_once(state, srv.origin, concurrency=1)
                self.assertEqual(result['status'], 'complete')
                ledger = Ledger(state)
                self.assertEqual(len(ledger.snapshot()['outlets']), 1)
                self.assertEqual(
                    [s['slot_id'] for s in ledger.query_outlet('A')['slots']], ['1', '2'])
                # The child link was visited once; no duplicated child chain.
                chains = [c for c in result['chains'] if c['kind'] == 'slots']
                self.assertEqual(len(chains), 1)

    # ---------------- version generations and late old pages ----------------
    def test_outlet_version_switch_builds_isolated_generations(self):
        with server('walk_version.json') as srv:
            with tempfile.TemporaryDirectory() as state:
                result = walk_once(state, srv.origin, max_pages=1000, concurrency=1)
                ledger = Ledger(state)
                snapshot = ledger.snapshot()
                gens = snapshot['outlets']
                self.assertEqual([(g['gen'], g['state']) for g in gens],
                                 [(1, 'superseded'), (2, 'complete')])
                current = ledger.query_outlet('A')
                self.assertEqual(current['gen'], 2)
                self.assertEqual(current['slot_version'], 's2')
                self.assertEqual([s['slot_id'] for s in current['slots']], ['9', '10'])
                # Old generation slots remain auditable but are not current data.
                with sqlite3.connect(state + '/walk.sqlite3') as db:
                    old = db.execute(
                        'SELECT slot_id FROM slots WHERE outlet_id=? AND gen=1 ORDER BY slot_id',
                        ('A',)).fetchall()
                self.assertEqual(old, [('1',)])
                old_chain = next(c for c in result['chains'] if c['gen'] == 1 and c['kind'] == 'slots')
                self.assertEqual(old_chain['state'], 'superseded')
                self.assertEqual(result['status'], 'complete')

    def test_late_old_version_child_page_is_kept_isolated_not_merged(self):
        # Deterministic ledger-level scenario: gen1 already superseded, then its
        # in-flight old-version response arrives late.
        with server('walk_version.json') as srv:
            with tempfile.TemporaryDirectory() as state:
                ledger = Ledger(state, srv.origin + '/outlets', srv.origin)
                root_payload = json.dumps({
                    'value': [{
                        'outletId': 'A', 'version': 'r1', 'slotVersion': 's1',
                        'Slots': [{'slotId': '1'}],
                        'Slots@odata.nextLink': '/slots/A?cursor=old'}],
                    '@odata.nextLink': '/outlets?cursor=b'}).encode()
                old_chain = 'slots:A#1#s1'
                ledger.apply_response('root', srv.origin + '/outlets', root_payload, 200)
                # New root page switches the outlet to gen 2 and supersedes gen 1.
                new_root = json.dumps({'value': [{
                    'outletId': 'A', 'version': 'r2', 'slotVersion': 's2',
                    'Slots': [{'slotId': '9'}],
                    'Slots@odata.nextLink': '/slots/A?cursor=new'}]}).encode()
                ledger.apply_response('root', srv.origin + '/outlets?cursor=b', new_root, 200)
                # The old child page now arrives late.
                late = json.dumps({'owner': 'A', 'version': 's1',
                                   'value': [{'slotId': '2'}]}).encode()
                next_url = ledger.apply_response(
                    old_chain, srv.origin + '/slots/A?cursor=old', late, 200)
                self.assertIsNone(next_url)
                snapshot = ledger.snapshot()
                self.assertEqual(len(snapshot['isolated_pages']), 1)
                isolated = snapshot['isolated_pages'][0]
                self.assertEqual(isolated['chain_id'], old_chain)
                self.assertEqual(isolated['observed_version'], 's1')
                current = ledger.query_outlet('A')
                self.assertEqual([s['slot_id'] for s in current['slots']], ['9'])
                # Old slot never leaked into the new generation.
                with sqlite3.connect(state + '/walk.sqlite3') as db:
                    new_slots = db.execute(
                        'SELECT slot_id FROM slots WHERE gen=2').fetchall()
                self.assertEqual(new_slots, [('9',)])
                ledger.close()

    # ---------------- bounded failures ----------------
    def test_continuation_loop_fails_within_budget_and_is_reported(self):
        with server('walk_loop.json') as srv:
            with tempfile.TemporaryDirectory() as state:
                result = walk_once(state, srv.origin, max_pages=50, concurrency=1)
                self.assertEqual(result['status'], 'failed')
                loop_chain = next(c for c in result['chains'] if c['chain_id'].startswith('slots:A'))
                self.assertEqual(loop_chain['state'], 'failed')
                self.assertIn('loop', loop_chain['reason'])
                # Root chain itself finished; the failure is scoped to the child.
                self.assertEqual(next(c for c in result['chains'] if c['kind'] == 'root')['state'],
                                 'complete')

    def test_missing_business_key_and_empty_next_link_are_bounded_failures(self):
        with server('walk_bad.json') as srv:
            with tempfile.TemporaryDirectory() as state:
                result = walk_once(state, srv.origin, max_pages=50, concurrency=1)
                self.assertEqual(result['status'], 'failed')
                root = next(c for c in result['chains'] if c['kind'] == 'root')
                self.assertEqual(root['state'], 'failed')
                self.assertIn('outletId', root['reason'])
        with server('walk_bad.json') as srv:
            with tempfile.TemporaryDirectory() as state:
                # Empty next link scenario: use ledger directly to reach the child.
                ledger = Ledger(state, srv.origin + '/outlets', srv.origin)
                first_root = json.dumps({'value': [{
                    'outletId': 'A', 'version': 'r1', 'slotVersion': 's1',
                    'Slots': [{'slotId': '1'}],
                    'Slots@odata.nextLink': '/slots/A?cursor=x'}]}).encode()
                ledger.apply_response('root', srv.origin + '/outlets', first_root, 200)
                bad_child = json.dumps(
                    {'owner': 'A', 'version': 's1', 'value': [],
                     '@odata.nextLink': '   '}).encode()
                ledger.apply_response('slots:A#1#s1',
                                      srv.origin + '/slots/A?cursor=x', bad_child, 200)
                chain = next(c for c in ledger.snapshot()['chains']
                             if c['chain_id'] == 'slots:A#1#s1')
                self.assertEqual(chain['state'], 'failed')
                self.assertIn('empty next link', chain['reason'])
                ledger.close()

    def test_cross_origin_next_link_is_rejected_before_request(self):
        with server('walk_cross_origin.json') as srv:
            with tempfile.TemporaryDirectory() as state:
                result = walk_once(state, srv.origin, max_pages=10, concurrency=1)
                self.assertEqual(result['status'], 'failed')
                child = next(c for c in result['chains'] if c['outlet_id'] == 'A')
                self.assertEqual(child['state'], 'failed')
                self.assertIn('origin', child['reason'])
                # Nothing was requested outside the local server.
                self.assertTrue(all('example.invalid' not in req['path']
                                    for req in srv.requests))

    def test_owner_mismatch_page_is_isolated_and_other_outlet_survives(self):
        with server('walk_owner_mismatch.json') as srv:
            with tempfile.TemporaryDirectory() as state:
                result = walk_once(state, srv.origin, max_pages=10, concurrency=1)
                self.assertEqual(result['status'], 'failed')
                ledger = Ledger(state)
                a = ledger.query_outlet('A')
                b = ledger.query_outlet('B')
                self.assertEqual(a['state'], 'failed')
                self.assertIn('mismatch', a['reason'])
                # One store's failed child chain must not undo a confirmed store.
                self.assertEqual(b['state'], 'complete')
                self.assertEqual(len(result['isolated_pages']), 1)
                self.assertEqual(result['isolated_pages'][0]['observed_owner'], 'WRONG')

    # ---------------- ordering does not decide identity ----------------
    def test_shuffled_http_completion_order_keeps_ownership(self):
        with server('walk_reorder.json') as srv:
            with tempfile.TemporaryDirectory() as state:
                result = walk_once(state, srv.origin, max_pages=50, concurrency=4)
                self.assertEqual(result['status'], 'complete')
                ledger = Ledger(state)
                self.assertEqual([s['slot_id'] for s in ledger.query_outlet('A')['slots']],
                                 ['1', '2'])
                self.assertEqual([s['slot_id'] for s in ledger.query_outlet('B')['slots']],
                                 ['1', '2'])
                # The slow A response really completes after B's; prove the
                # completion order differed while attribution stayed parent-scoped.
                completed = srv.completions
                self.assertLess(completed.index('/slots/B?cursor=x'),
                                completed.index('/slots/A?cursor=x'))

    def test_deterministic_ledger_apply_order_invariance(self):
        # Feed the same pages through two ledgers in opposite orders.
        root = json.dumps({'value': [
            {'outletId': 'A', 'version': 'r1', 'slotVersion': 's1',
             'Slots': [{'slotId': '1'}], 'Slots@odata.nextLink': '/slots/A?cursor=x'},
            {'outletId': 'B', 'version': 'r1', 'slotVersion': 's1',
             'Slots': [{'slotId': '1'}], 'Slots@odata.nextLink': '/slots/B?cursor=x'}]})
        page_a = json.dumps({'owner': 'A', 'version': 's1',
                             'value': [{'slotId': '7'}]}).encode()
        page_b = json.dumps({'owner': 'B', 'version': 's1',
                             'value': [{'slotId': '7'}]}).encode()
        outcomes = []
        for order in (('A', 'B'), ('B', 'A')):
            with tempfile.TemporaryDirectory() as state:
                ledger = Ledger(state, 'http://127.0.0.1:9/outlets', 'http://127.0.0.1:9')
                ledger.apply_response('root', 'http://127.0.0.1:9/outlets',
                                      root.encode(), 200)
                for name in order:
                    ledger.apply_response(
                        'slots:' + name + '#1#s1',
                        'http://127.0.0.1:9/slots/' + name + '?cursor=x',
                        page_a if name == 'A' else page_b, 200)
                snap = ledger.snapshot()
                outcomes.append(sorted(
                    (c['outlet_id'], c['state']) for c in snap['chains'] if c['kind'] == 'slots'))
                ledger.close()
        self.assertEqual(outcomes[0], outcomes[1])

    # ---------------- real crash and restart ----------------
    def test_subprocess_crash_then_resume_uses_persistent_ledger(self):
        # Fixed port so the subprocess can reach the in-process replay server.
        server_obj = ReplayServer(FIXTURES, 'walk.json', port=0)
        thread = threading.Thread(target=server_obj.serve_forever, daemon=True)
        thread.start()
        try:
            origin = server_obj.origin
            with tempfile.TemporaryDirectory() as state:
                project = str(Path(__file__).resolve().parents[1])
                env = dict(os.environ)
                env['PYTHONPATH'] = project + (os.pathsep + env['PYTHONPATH']
                                               if env.get('PYTHONPATH') else '')
                cmd = [sys.executable, '-P', '-m', 'outletwalk', 'walk',
                       origin + '/outlets', '--state', state, '--origin', origin,
                       '--max-pages', '2', '--concurrency', '1']
                proc = subprocess.run(cmd, capture_output=True, env=env, timeout=30)
                self.assertEqual(proc.returncode, 0, proc.stderr.decode())
                paused = json.loads(proc.stdout.decode())
                self.assertEqual(paused['status'], 'paused')
                # Now kill a resumed process mid-flight for real, then resume again.
                cmd_kill = [sys.executable, '-P', '-m', 'outletwalk', 'resume',
                            '--state', state, '--max-pages', '1', '--concurrency', '1']
                killed = subprocess.Popen(cmd_kill, env=env, stdout=subprocess.PIPE,
                                          stderr=subprocess.PIPE)
                killed.terminate()
                killed.wait(timeout=30)
                self.assertNotEqual(killed.returncode, 0)
                # The ledger survives the kill and still explains the gap.
                view = ledger_view(state)
                try:
                    self.assertEqual(view.snapshot()['status'], 'paused')
                finally:
                    view.close()
                resume_cmd = [sys.executable, '-P', '-m', 'outletwalk', 'resume',
                              '--state', state, '--max-pages', '10', '--concurrency', '2']
                finished = subprocess.run(resume_cmd, capture_output=True, env=env, timeout=30)
                self.assertEqual(finished.returncode, 0, finished.stderr.decode())
                snapshot = json.loads(finished.stdout.decode())
                self.assertEqual(snapshot['status'], 'complete')
                query_cmd = [sys.executable, '-P', '-m', 'outletwalk', 'query',
                             '--state', state, 'A']
                queried = subprocess.run(query_cmd, capture_output=True, env=env, timeout=30)
                self.assertEqual(queried.returncode, 0)
                self.assertEqual(
                    [s['slot_id'] for s in json.loads(queried.stdout.decode())['slots']],
                    ['1', '2'])
        finally:
            server_obj.shutdown()
            server_obj.server_close()
            thread.join()

    # ---------------- opaque relative links and origin config ----------------
    def test_relative_link_resolved_against_response_address(self):
        # The /outlets?cursor=b link is relative and must be resolved against the
        # root response URL, not reconstructed with a hand-built skip.
        with server('walk.json') as srv:
            with tempfile.TemporaryDirectory() as state:
                walk_once(state, srv.origin, max_pages=1, concurrency=1)
                with sqlite3.connect(state + '/walk.sqlite3') as db:
                    queued = db.execute(
                        "SELECT url FROM chains WHERE chain_id='root'").fetchone()[0]
                self.assertEqual(queued, srv.origin + '/outlets?cursor=b')

    def test_walk_rejects_root_outside_allowed_origin(self):
        with tempfile.TemporaryDirectory() as state:
            with self.assertRaises(Exception):
                Ledger(state, 'http://example.invalid/outlets', 'http://127.0.0.1:9')

    # ---------------- ledger is the durable completeness explanation ----------------
    def test_completeness_is_durable_sqlite_state_not_in_memory_count(self):
        with server('walk.json') as srv:
            with tempfile.TemporaryDirectory() as state:
                walk_once(state, srv.origin, max_pages=2, concurrency=1)
                db_path = state + '/walk.sqlite3'
                self.assertTrue(Path(db_path).exists())
                with sqlite3.connect(db_path) as db:
                    tables = {row[0] for row in db.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'")}
                self.assertIn('chains', tables)
                self.assertIn('pages', tables)
                self.assertIn('visited', tables)
                self.assertIn('slots', tables)
                self.assertIn('outlets', tables)
                self.assertIn('isolated_pages', tables)
                with sqlite3.connect(db_path) as db:
                    page_count = db.execute(
                        "SELECT COUNT(*) FROM pages WHERE kind='http'").fetchone()[0]
                    inline_count = db.execute(
                        "SELECT COUNT(*) FROM pages WHERE kind='inline'").fetchone()[0]
                self.assertEqual(page_count, 2)  # exactly the two root pages so far
                # Inline child first pages are ledger entries, not HTTP fetches.
                self.assertGreaterEqual(inline_count, 1)


if __name__ == '__main__':
    unittest.main()
