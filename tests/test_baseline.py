import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from outletwalk.parser import inspect
from outletwalk.journal import Journal
from outletwalk.transport import capture
from scripts.materialize import main as materialize
from scripts.source_server import running


class BaselineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        materialize()

    def test_capture_journal_is_content_addressed(self):
        with tempfile.TemporaryDirectory() as directory:
            journal = Journal(directory)
            try:
                first = journal.save(b'synthetic', 'source-1')
                second = journal.save(b'synthetic', 'source-1')
                self.assertEqual(first, second)
                self.assertEqual(Path(first['path']).read_bytes(), b'synthetic')
                self.assertEqual(journal.database.execute('SELECT COUNT(*) FROM captures').fetchone()[0], 1)
            finally:
                journal.close()

    def test_remote_transport_is_rejected_before_io(self):
        with self.assertRaises(ValueError):
            capture('https://example.invalid/private')

    def test_cli_help(self):
        result = subprocess.run([sys.executable, '-m', 'outletwalk', '--help'], capture_output=True)
        self.assertEqual(result.returncode, 0)
        self.assertIn(b'peek', result.stdout)

    def test_local_source_and_request_log(self):
        with running() as server:
            status, headers, payload = capture(server.origin + '/outlets', 'GET', headers={})
            self.assertEqual(status, 200)
            self.assertGreater(len(payload), 0)
            self.assertEqual(server.requests[0]['method'], 'GET')

    def test_unknown_source_route_is_not_success(self):
        with running() as server:
            self.assertEqual(capture(server.origin + '/not-present')[0], 404)

    def test_nested_link_is_preserved(self):
        result = inspect(Path('fixtures/outlets.json').read_bytes())
        self.assertEqual(result['records'][0]['Slots@odata.nextLink'], '/slots/A?cursor=x')
        self.assertEqual(result['next'], '/outlets?cursor=b')

    def test_missing_collection_rejected(self):
        with self.assertRaises(ValueError):
            inspect(b'{"records": []}')

    def test_parent_scope_in_source_pages(self):
        with running() as server:
            child = json.loads(capture(server.origin + '/slots/A?cursor=x')[2])
            self.assertEqual(child['owner'], 'A')
            self.assertEqual(child['version'], 's1')


if __name__ == '__main__':
    unittest.main()
