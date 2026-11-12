import json
from pathlib import Path
import subprocess
import sys
import tempfile
from .materialize import main as materialize
from .source_server import running


def main():
    materialize()
    subprocess.run([sys.executable, '-m', 'outletwalk', 'peek', 'fixtures/outlets.json'], check=True)
    with running() as server, tempfile.TemporaryDirectory() as state:
        subprocess.run([sys.executable, '-m', 'outletwalk', 'capture', server.origin + '/outlets', '--method', 'GET', '--state', state, *[]], check=True)
        print(json.dumps({'requests': server.requests}, ensure_ascii=True))


if __name__ == '__main__':
    main()
