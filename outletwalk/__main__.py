import argparse
import json
from pathlib import Path
import sys
from .parser import inspect
from .journal import Journal
from .transport import capture
from .walker import Walker
from .ledger import Ledger, LedgerError


def _print(value):
    print(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True))


def main():
    parser = argparse.ArgumentParser(description='品牌门店与服务时段 OData 页面查看器。')
    sub = parser.add_subparsers(dest='command', required=True)
    view = sub.add_parser('peek')
    view.add_argument('source')
    raw = sub.add_parser('capture')
    raw.add_argument('url')
    raw.add_argument('--state', default='var')
    raw.add_argument('--method', default='GET')
    raw.add_argument('--body')
    raw.add_argument('--header', action='append', default=[])
    walk = sub.add_parser('walk', help='采集根链与每家门店 Slots 子链，并持久化完整性账本')
    walk.add_argument('url')
    walk.add_argument('--state', required=True)
    walk.add_argument('--origin', required=True, help='允许的来源 origin，续页解析后不得超出')
    walk.add_argument('--max-pages', type=int, default=1000)
    walk.add_argument('--concurrency', type=int, default=2)
    resume = sub.add_parser('resume', help='基于已有账本重启续行未结束的链')
    resume.add_argument('--state', required=True)
    resume.add_argument('--max-pages', type=int, default=None)
    resume.add_argument('--concurrency', type=int, default=None)
    status = sub.add_parser('status', help='查看整次采集与每条父子链的完整状态')
    status.add_argument('--state', required=True)
    query = sub.add_parser('query', help='按门店身份查询当前代际的已确认时段')
    query.add_argument('--state', required=True)
    query.add_argument('outlet_id')
    args = parser.parse_args()
    try:
        if args.command == 'capture':
            headers = dict(value.split(':', 1) for value in args.header)
            headers = {key.strip(): value.strip() for key, value in headers.items()}
            status_code, response_headers, payload = capture(args.url, args.method,
                Path(args.body).read_bytes() if args.body else None, headers)
            journal = Journal(args.state)
            try:
                result = journal.save(payload, args.url)
            finally:
                journal.close()
            result.update({'http_status': status_code, 'headers': response_headers})
            print(json.dumps(result, ensure_ascii=True, sort_keys=True))
        elif args.command == 'peek':
            result = inspect(Path(args.source).read_bytes())
            print(json.dumps(result, ensure_ascii=True, sort_keys=True))
        elif args.command == 'walk':
            walker = Walker(args.state, args.url, args.origin.rstrip('/'),
                            args.max_pages, args.concurrency)
            try:
                _print(walker.run())
            finally:
                walker.close()
        elif args.command == 'resume':
            walker = Walker(args.state)
            if args.max_pages is not None:
                walker.max_pages = args.max_pages
            if args.concurrency is not None:
                walker.concurrency = args.concurrency
            try:
                _print(walker.run())
            finally:
                walker.close()
        elif args.command == 'status':
            ledger = Ledger(args.state)
            try:
                _print(ledger.snapshot())
            finally:
                ledger.close()
        elif args.command == 'query':
            ledger = Ledger(args.state)
            try:
                result = ledger.query_outlet(args.outlet_id)
            finally:
                ledger.close()
            if result is None:
                print('unknown outlet: ' + args.outlet_id, file=sys.stderr)
                return 2
            _print(result)
    except (ValueError, OSError, LedgerError) as error:
        print(str(error), file=sys.stderr)
        return 2
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
