"""End-to-end business demo for the outlet walk (kept under 90 seconds).

The scenario deliberately leaves one store's Slots child chain open after the
root catalogue has fully ended, then resumes and completes it.  The two phases
are readable in the persisted SQLite ledger and through the query command, so
the channel team can distinguish "this store is fully confirmed" from "the
whole run finished".  The original peek/capture output stays available.
"""

import json
from pathlib import Path
import subprocess
import sys
import tempfile

from .materialize import main as materialize
from .source_server import running


def _run(cmd):
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    return result.stdout.strip()


def main():
    materialize()
    print('== 1) 既有 peek 输出保持可用 ==')
    subprocess.run([sys.executable, '-m', 'outletwalk', 'peek', 'fixtures/outlets.json'], check=True)

    with running(root='fixtures', scenario='walk.json') as server, \
            tempfile.TemporaryDirectory() as state:
        origin = server.origin
        root = origin + '/outlets'
        print('\n== 2) 首轮采集：预算只够两条根页，外层结束但 A 的子链未结束 ==')
        first = json.loads(_run([
            sys.executable, '-m', 'outletwalk', 'walk', root,
            '--state', state, '--origin', origin,
            '--max-pages', '2', '--concurrency', '1']))
        print('整次采集状态:', first['status'], '-', first['reason'])
        for chain in first['chains']:
            label = chain['chain_id']
            print('  链', label, '=>', chain['state'],
                  '(' + chain['reason'] + ')' if chain['reason'] else '',
                  '待取:', chain['url'] or '-')
        print('已确认完整门店 B 可供查询:')
        print(_run([sys.executable, '-m', 'outletwalk', 'query', '--state', state, 'B']))
        print('未取齐门店 A 说明卡在哪一页:')
        print(_run([sys.executable, '-m', 'outletwalk', 'query', '--state', state, 'A']))

        print('\n== 3) 重启续行：基于持久化账本补齐 A 剩余子页 ==')
        second = json.loads(_run([
            sys.executable, '-m', 'outletwalk', 'resume',
            '--state', state, '--max-pages', '10', '--concurrency', '2']))
        print('整次采集状态:', second['status'])
        print('补齐后查询门店 A 的完整周末时段:')
        print(_run([sys.executable, '-m', 'outletwalk', 'query', '--state', state, 'A']))

        print('\n== 4) 完整性账本（SQLite）核对 ==')
        print(_run([sys.executable, '-m', 'outletwalk', 'status', '--state', state]))
        print('\n演示请求记录:')
        print(json.dumps(server.requests, ensure_ascii=False))


if __name__ == '__main__':
    main()
