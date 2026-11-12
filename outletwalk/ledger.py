"""Persistent parent/child collection ledger for the outlet walk.

The ledger is the only source of truth for walk completeness.  Every fetched
page, every continuation link and every observed version is committed to
SQLite before the in-process scheduler forgets about it, so a restart can
resume from the open chains instead of relying on an in-memory todo count.

Root pages and each outlet's Slots pages are independent chains with their own
next-link and their own version.  A slot is only ever attributed through its
parent outlet identity and the child collection version it was confirmed under;
response order never decides ownership.
"""

from datetime import datetime, timezone
import hashlib
import json
import sqlite3
import threading
from pathlib import Path
from urllib.parse import urljoin, urlsplit

from .transport import origin_tuple

ROOT_CHAIN = 'root'


def _now():
    return datetime.now(timezone.utc).isoformat()


def canonical(value):
    """Stable JSON text used for content digests and conflict comparison."""
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'))


def digest_of(payload):
    if isinstance(payload, str):
        payload = payload.encode('utf-8')
    return hashlib.sha256(payload).hexdigest()


class LedgerError(Exception):
    """Raised for configuration problems that make a walk impossible."""


class Ledger:
    def __init__(self, state_dir, root_url=None, allowed_origin=None,
                 max_pages=1000, concurrency=2):
        self.lock = threading.RLock()
        self.state_dir = state_dir
        Path(state_dir).mkdir(parents=True, exist_ok=True)
        # All access is serialized by self.lock; the same connection is shared
        # across the bounded worker pool.
        self.database = sqlite3.connect(str(state_dir) + '/walk.sqlite3',
                                        check_same_thread=False)
        self.database.row_factory = sqlite3.Row
        self.database.execute('PRAGMA journal_mode=WAL')
        self.database.execute('PRAGMA foreign_keys=ON')
        self._create_schema()
        with self.lock:
            row = self.database.execute('SELECT value FROM walk_meta WHERE key=?', ('config',)).fetchone()
            if row is None:
                if not root_url or not allowed_origin:
                    raise LedgerError('A new walk ledger needs root_url and allowed_origin')
                self._validate_origin(root_url, allowed_origin)
                self.database.execute(
                    'INSERT INTO walk_meta(key,value) VALUES(?,?)',
                    ('config', canonical({'root_url': root_url, 'allowed_origin': allowed_origin,
                                          'max_pages': int(max_pages), 'concurrency': int(concurrency)})))
                self._set_meta('status', 'running')
                self._set_meta('budget_used', '0')
                self.database.execute(
                    'INSERT INTO chains(chain_id, kind, outlet_id, gen, expected_version, '
                    'url, state, reason, ordinal, created_at, updated_at) '
                    'VALUES(?,?,?,?,?,?,?,?,?,?,?)',
                    (ROOT_CHAIN, 'root', None, 0, None, root_url, 'open', None, 0, _now(), _now()))
                self.database.execute('INSERT INTO visited(chain_id, url) VALUES(?,?)',
                                      (ROOT_CHAIN, root_url))
                self.database.commit()
            else:
                config = json.loads(row['value'])
                root_url = config['root_url']
                allowed_origin = config['allowed_origin']
                max_pages = config['max_pages']
                concurrency = config['concurrency']
                # A previous process may have died while holding claims.
                self.database.execute('DELETE FROM claims')
                self.database.commit()
        self.root_url = root_url
        self.allowed_origin = allowed_origin
        self.max_pages = max_pages
        self.concurrency = concurrency

    def update_run_config(self, max_pages=None, concurrency=None):
        """Record this process run's budget/concurrency without touching history."""
        with self.lock:
            if max_pages is not None:
                self.max_pages = int(max_pages)
            if concurrency is not None:
                self.concurrency = int(concurrency)
            self.database.execute(
                'INSERT INTO walk_meta(key,value) VALUES(?,?) '
                'ON CONFLICT(key) DO UPDATE SET value=excluded.value',
                ('config', canonical({'root_url': self.root_url,
                                      'allowed_origin': self.allowed_origin,
                                      'max_pages': int(self.max_pages),
                                      'concurrency': int(self.concurrency)})))
            self.database.commit()

    # ------------------------------------------------------------------ schema
    def _create_schema(self):
        with self.lock:
            self.database.executescript(
                """
                CREATE TABLE IF NOT EXISTS walk_meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS chains (
                    chain_id TEXT PRIMARY KEY,
                    kind TEXT NOT NULL CHECK (kind IN ('root','slots')),
                    outlet_id TEXT,
                    gen INTEGER NOT NULL,
                    expected_version TEXT,
                    url TEXT,
                    state TEXT NOT NULL CHECK (state IN ('open','complete','failed','superseded')),
                    reason TEXT,
                    ordinal INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS pages (
                    chain_id TEXT NOT NULL,
                    ordinal INTEGER NOT NULL,
                    url TEXT NOT NULL,
                    digest TEXT NOT NULL,
                    kind TEXT NOT NULL CHECK (kind IN ('http','inline')),
                    next_link TEXT,
                    observed_version TEXT,
                    fetched_at TEXT NOT NULL,
                    PRIMARY KEY (chain_id, ordinal)
                );
                CREATE TABLE IF NOT EXISTS visited (
                    chain_id TEXT NOT NULL,
                    url TEXT NOT NULL,
                    PRIMARY KEY (chain_id, url)
                );
                CREATE TABLE IF NOT EXISTS isolated_pages (
                    chain_id TEXT NOT NULL,
                    url TEXT NOT NULL,
                    digest TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    observed_owner TEXT,
                    observed_version TEXT,
                    at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS outlets (
                    outlet_id TEXT NOT NULL,
                    gen INTEGER NOT NULL,
                    root_version TEXT NOT NULL,
                    slot_version TEXT NOT NULL,
                    record_digest TEXT NOT NULL,
                    state TEXT NOT NULL CHECK (state IN ('open','complete','failed','superseded')),
                    reason TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (outlet_id, gen)
                );
                CREATE TABLE IF NOT EXISTS slots (
                    outlet_id TEXT NOT NULL,
                    gen INTEGER NOT NULL,
                    slot_version TEXT NOT NULL,
                    slot_id TEXT NOT NULL,
                    record_digest TEXT NOT NULL,
                    first_seen_chain TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (outlet_id, gen, slot_id)
                );
                CREATE TABLE IF NOT EXISTS claims (
                    chain_id TEXT PRIMARY KEY,
                    token TEXT NOT NULL,
                    claimed_at TEXT NOT NULL
                );
                """)
            self.database.commit()

    # ------------------------------------------------------------------- helpers
    def _set_meta(self, key, value):
        self.database.execute(
            'INSERT INTO walk_meta(key,value) VALUES(?,?) '
            'ON CONFLICT(key) DO UPDATE SET value=excluded.value', (key, str(value)))

    def _get_meta(self, key):
        row = self.database.execute('SELECT value FROM walk_meta WHERE key=?', (key,)).fetchone()
        return row['value'] if row else None

    @staticmethod
    def _validate_origin(url, allowed_origin):
        parsed_origin = urlsplit(allowed_origin)
        if parsed_origin.scheme != 'http' or not parsed_origin.hostname:
            raise LedgerError('allowed_origin must be an http origin')
        if origin_tuple(url) != origin_tuple(allowed_origin):
            raise LedgerError('root_url must stay inside allowed_origin')

    def _resolve_link(self, base_url, link):
        """Resolve an opaque next link against the response address.

        Links are never hand-built: urljoin keeps the full query string.  The
        resolved URL must remain inside the configured origin.
        """
        if not isinstance(link, str) or not link.strip():
            return None, 'empty next link'
        resolved = urljoin(base_url, link)
        if origin_tuple(resolved) != origin_tuple(self.allowed_origin):
            return None, 'next link escapes allowed origin: ' + resolved
        if urlsplit(resolved).username or urlsplit(resolved).password:
            return None, 'credentials in next link are not accepted'
        return resolved, None

    def _fail_chain(self, chain_id, reason):
        self.database.execute(
            "UPDATE chains SET state='failed', reason=?, url=url, updated_at=? WHERE chain_id=?",
            (reason, _now(), chain_id))
        row = self.database.execute(
            'SELECT kind, outlet_id, gen FROM chains WHERE chain_id=?', (chain_id,)).fetchone()
        if row and row['kind'] == 'slots':
            self.database.execute(
                "UPDATE outlets SET state='failed', reason=?, updated_at=? "
                'WHERE outlet_id=? AND gen=? AND state=?',
                (reason, _now(), row['outlet_id'], row['gen'], 'open'))

    def _supersede_chain(self, chain_id, reason):
        self.database.execute(
            "UPDATE chains SET state='superseded', reason=?, updated_at=? WHERE chain_id=? AND state='open'",
            (reason, _now(), chain_id))

    def _advance_chain(self, chain_id, next_url, next_link, observed_version):
        if next_url is None:
            self.database.execute(
                "UPDATE chains SET state='complete', url=NULL, reason=?, updated_at=? WHERE chain_id=?",
                (next_link, _now(), chain_id))
            row = self.database.execute(
                'SELECT kind, outlet_id, gen FROM chains WHERE chain_id=?', (chain_id,)).fetchone()
            if row and row['kind'] == 'slots':
                self.database.execute(
                    "UPDATE outlets SET state='complete', reason=NULL, updated_at=? "
                    'WHERE outlet_id=? AND gen=?',
                    (_now(), row['outlet_id'], row['gen']))
            return
        self.database.execute(
            "UPDATE chains SET url=?, state='open', reason=NULL, updated_at=? WHERE chain_id=?",
            (next_url, _now(), chain_id))
        self.database.execute('INSERT OR IGNORE INTO visited(chain_id,url) VALUES(?,?)',
                              (chain_id, next_url))

    def _next_ordinal(self, chain_id):
        row = self.database.execute(
            'SELECT COALESCE(MAX(ordinal), -1) + 1 AS n FROM pages WHERE chain_id=?',
            (chain_id,)).fetchone()
        return row['n']

    # ------------------------------------------------------------- seeding pages
    def seed_inline_page(self, chain_id, url, document, observed_version):
        """Record a Slots first page that arrived inline inside a root record."""
        with self.lock:
            exists = self.database.execute(
                'SELECT 1 FROM pages WHERE chain_id=? AND kind=?', (chain_id, 'inline')).fetchone()
            if exists:
                return
            ordinal = self._next_ordinal(chain_id)
            self.database.execute(
                'INSERT INTO pages(chain_id, ordinal, url, digest, kind, next_link, '
                'observed_version, fetched_at) VALUES(?,?,?,?,?,?,?,?)',
                (chain_id, ordinal, url, digest_of(canonical(document)), 'inline',
                 document.get('@odata.nextLink'), observed_version, _now()))
            for slot in document.get('value', []):
                self._store_slot(chain_id, slot)
            self.database.commit()

    def _store_slot(self, chain_id, slot):
        row = self.database.execute(
            'SELECT kind, outlet_id, gen, expected_version FROM chains WHERE chain_id=?',
            (chain_id,)).fetchone()
        if not row or row['kind'] != 'slots':
            return
        outlet_id, gen, slot_version = row['outlet_id'], row['gen'], row['expected_version']
        if not isinstance(slot, dict) or not slot.get('slotId'):
            raise ValueError('slot record without business key slotId')
        self.database.execute(
            'INSERT OR IGNORE INTO slots(outlet_id, gen, slot_version, slot_id, '
            'record_digest, first_seen_chain, created_at) VALUES(?,?,?,?,?,?,?)',
            (outlet_id, gen, slot_version, str(slot['slotId']),
             digest_of(canonical(slot)), chain_id, _now()))

    # ------------------------------------------------------------- child chains
    def _ensure_child_chain(self, outlet_id, gen, slot_version, first_url):
        chain_id = self._child_chain_id(outlet_id, gen, slot_version)
        row = self.database.execute('SELECT 1 FROM chains WHERE chain_id=?', (chain_id,)).fetchone()
        if row:
            return chain_id
        self.database.execute(
            'INSERT INTO chains(chain_id, kind, outlet_id, gen, expected_version, url, '
            "state, reason, ordinal, created_at, updated_at) VALUES(?,?,?,?,?,?,'open',NULL,0,?,?)",
            (chain_id, 'slots', outlet_id, gen, slot_version, first_url, _now(), _now()))
        self.database.execute('INSERT OR IGNORE INTO visited(chain_id,url) VALUES(?,?)',
                              (chain_id, first_url))
        return chain_id

    @staticmethod
    def _child_chain_id(outlet_id, gen, slot_version):
        return 'slots:' + outlet_id + '#' + str(gen) + '#' + str(slot_version)

    # ------------------------------------------------------------- root records
    def _process_root_record(self, record, page_url):
        if not isinstance(record, dict) or not record.get('outletId'):
            raise ValueError('root record without business key outletId')
        outlet_id = str(record['outletId'])
        root_version = record.get('version')
        slot_version = record.get('slotVersion')
        if not root_version or not slot_version:
            raise ValueError('outlet record without version/slotVersion')
        record_digest = digest_of(canonical(record))
        latest = self.database.execute(
            'SELECT gen, root_version, record_digest, state FROM outlets WHERE outlet_id=? '
            'ORDER BY gen DESC LIMIT 1', (outlet_id,)).fetchone()
        if latest is None:
            gen = 1
            self.database.execute(
                'INSERT INTO outlets(outlet_id, gen, root_version, slot_version, record_digest, '
                "state, reason, created_at, updated_at) VALUES(?,?,?,?,?,'open',NULL,?,?)",
                (outlet_id, gen, root_version, slot_version, record_digest, _now(), _now()))
        elif latest['root_version'] == root_version:
            gen = latest['gen']
            if latest['record_digest'] != record_digest:
                raise ValueError('conflicting outlet record at equal version for ' + outlet_id)
            # Same record repeated on another root page: nothing new to seed.
            return gen
        else:
            # New observation of the outlet: a new independent child generation.
            gen = latest['gen'] + 1
            self.database.execute(
                'INSERT INTO outlets(outlet_id, gen, root_version, slot_version, record_digest, '
                "state, reason, created_at, updated_at) VALUES(?,?,?,?,?,'open',NULL,?,?)",
                (outlet_id, gen, root_version, slot_version, record_digest, _now(), _now()))
            old_rows = self.database.execute(
                'SELECT chain_id FROM chains WHERE outlet_id=? AND gen=? AND kind=?',
                (outlet_id, gen - 1, 'slots')).fetchall()
            for old in old_rows:
                self._supersede_chain(old['chain_id'], 'outlet entered a new root version')
            self.database.execute(
                "UPDATE outlets SET state='superseded', reason=?, updated_at=? "
                'WHERE outlet_id=? AND gen=?',
                ('superseded by gen ' + str(gen), _now(), outlet_id, gen - 1))
        inline = record.get('Slots')
        next_link = record.get('Slots@odata.nextLink')
        chain_id = self._child_chain_id(outlet_id, gen, slot_version)
        if inline is not None:
            if not isinstance(inline, list):
                raise ValueError('inline Slots must be an array for ' + outlet_id)
            if not next_link:
                # The inline page is the whole child collection: finish the
                # chain immediately, never enqueue a fragment URL for fetching.
                self._ensure_complete_child(chain_id, outlet_id, gen, slot_version,
                                            'inline collection has no continuation')
                inline_document = {'value': inline}
                self.seed_inline_page(chain_id, page_url, inline_document, slot_version)
                self.database.execute(
                    "UPDATE outlets SET state='complete', reason=NULL, updated_at=? "
                    'WHERE outlet_id=? AND gen=?', (_now(), outlet_id, gen))
            else:
                first_url, problem = self._resolve_link(page_url, next_link)
                if problem:
                    self._ensure_failed_child(chain_id, outlet_id, gen, slot_version, problem)
                else:
                    self._ensure_child_chain(outlet_id, gen, slot_version, first_url)
                    inline_document = {'value': inline, '@odata.nextLink': next_link}
                    self.seed_inline_page(chain_id, page_url, inline_document, slot_version)
        elif next_link:
            # No inline first page: the child collection starts at its own link.
            first_url, problem = self._resolve_link(page_url, next_link)
            if problem:
                self._ensure_failed_child(chain_id, outlet_id, gen, slot_version, problem)
            else:
                self._ensure_child_chain(outlet_id, gen, slot_version, first_url)
        else:
            # No slots and no continuation: the child collection is empty/finished.
            self._ensure_complete_child(chain_id, outlet_id, gen, slot_version,
                                        'no slot collection')
            self.database.execute(
                "UPDATE outlets SET state='complete', reason=NULL, updated_at=? "
                'WHERE outlet_id=? AND gen=?', (_now(), outlet_id, gen))
        return gen

    def _ensure_complete_child(self, chain_id, outlet_id, gen, slot_version, reason):
        if self.database.execute('SELECT 1 FROM chains WHERE chain_id=?',
                                 (chain_id,)).fetchone():
            return
        self.database.execute(
            'INSERT INTO chains(chain_id, kind, outlet_id, gen, expected_version, '
            "url, state, reason, ordinal, created_at, updated_at) "
            "VALUES(?,?,?,?,?,NULL,'complete',?,0,?,?)",
            (chain_id, 'slots', outlet_id, gen, slot_version, reason, _now(), _now()))

    def _ensure_failed_child(self, chain_id, outlet_id, gen, slot_version, reason):
        """Fail one outlet's child chain without failing the root or siblings."""
        if self.database.execute('SELECT 1 FROM chains WHERE chain_id=?',
                                 (chain_id,)).fetchone():
            self._fail_chain(chain_id, reason)
            return
        self.database.execute(
            'INSERT INTO chains(chain_id, kind, outlet_id, gen, expected_version, '
            "url, state, reason, ordinal, created_at, updated_at) "
            "VALUES(?,?,?,?,?,NULL,'failed',?,0,?,?)",
            (chain_id, 'slots', outlet_id, gen, slot_version, reason, _now(), _now()))
        self.database.execute(
            "UPDATE outlets SET state='failed', reason=?, updated_at=? "
            'WHERE outlet_id=? AND gen=?', (reason, _now(), outlet_id, gen))

    # ------------------------------------------------------------- HTTP responses
    def apply_response(self, chain_id, url, payload, status):
        """Commit one fetched page and move its chain forward.

        Returns the next URL to fetch (or None when the chain is terminal).
        All identity/version checks happen here, inside the ledger transaction.
        """
        with self.lock:
            chain = self.database.execute('SELECT * FROM chains WHERE chain_id=?',
                                          (chain_id,)).fetchone()
            if chain is None:
                raise LedgerError('unknown chain ' + chain_id)
            if chain['state'] != 'open':
                # Late page for a terminal (complete/failed/superseded) chain:
                # keep it in isolation with its real owner/version, never splice
                # it into the current generation.
                late_owner = late_version = None
                try:
                    late_document = json.loads(payload.decode('utf-8'))
                    if isinstance(late_document, dict):
                        late_owner = late_document.get('owner')
                        late_version = late_document.get('version')
                except (UnicodeDecodeError, json.JSONDecodeError):
                    pass
                self._isolate(chain_id, url, payload,
                              'page arrived for ' + chain['state'] + ' chain',
                              late_owner, late_version)
                self.database.commit()
                return None
            if status != 200:
                self._fail_chain(chain_id, 'HTTP status ' + str(status))
                self.database.commit()
                return None
            try:
                document = json.loads(payload.decode('utf-8'))
            except (UnicodeDecodeError, json.JSONDecodeError) as error:
                self._fail_chain(chain_id, 'invalid JSON: ' + str(error))
                self.database.commit()
                return None
            if not isinstance(document, dict) or not isinstance(document.get('value'), list):
                self._fail_chain(chain_id, 'expected collection value array')
                self.database.commit()
                return None
            observed_version = document.get('version')
            ordinal = self._next_ordinal(chain_id)
            self.database.execute(
                'INSERT INTO pages(chain_id, ordinal, url, digest, kind, next_link, '
                'observed_version, fetched_at) VALUES(?,?,?,?,?,?,?,?)',
                (chain_id, ordinal, url, digest_of(payload), 'http',
                 document.get('@odata.nextLink'), observed_version, _now()))
            if chain['kind'] == 'root':
                try:
                    for record in document['value']:
                        self._process_root_record(record, url)
                except ValueError as error:
                    self._fail_chain(chain_id, str(error))
                    self.database.commit()
                    return None
            else:
                owner = document.get('owner')
                if owner != chain['outlet_id'] or observed_version != chain['expected_version']:
                    self._isolate(chain_id, url, payload,
                                  'owner/version mismatch', owner, observed_version)
                    # The chain cannot continue on a foreign-version page.
                    self._fail_chain(chain_id,
                                     'child page owner/version mismatch (owner=%r version=%r)'
                                     % (owner, observed_version))
                    self.database.commit()
                    return None
                try:
                    for slot in document['value']:
                        self._store_slot(chain_id, slot)
                except ValueError as error:
                    self._fail_chain(chain_id, str(error))
                    self.database.commit()
                    return None
            raw_next = document.get('@odata.nextLink')
            if raw_next is None:
                next_url = None
            else:
                next_url, problem = self._resolve_link(url, raw_next)
                if problem:
                    self._fail_chain(chain_id, problem)
                    self.database.commit()
                    return None
                seen = self.database.execute(
                    'SELECT 1 FROM visited WHERE chain_id=? AND url=?',
                    (chain_id, next_url)).fetchone()
                if seen:
                    self._fail_chain(chain_id, 'continuation loop at ' + next_url)
                    self.database.commit()
                    return None
            self._advance_chain(chain_id, next_url, raw_next, observed_version)
            self.database.commit()
            return next_url

    def _isolate(self, chain_id, url, payload, reason, owner, version):
        self.database.execute(
            'INSERT INTO isolated_pages(chain_id, url, digest, reason, observed_owner, '
            'observed_version, at) VALUES(?,?,?,?,?,?,?)',
            (chain_id, url, digest_of(payload), reason, owner, version, _now()))

    def mark_chain_failed(self, chain_id, reason):
        with self.lock:
            self._fail_chain(chain_id, reason)
            self.database.commit()

    # ------------------------------------------------------------- scheduling
    def claim_next(self, token):
        """Claim one open chain for a worker; claims survive nothing but a process."""
        with self.lock:
            rows = self.database.execute(
                "SELECT chain_id FROM chains WHERE state='open' AND url IS NOT NULL "
                'AND chain_id NOT IN (SELECT chain_id FROM claims) ORDER BY chain_id').fetchall()
            if not rows:
                return None
            chain_id = rows[0]['chain_id']
            self.database.execute(
                'INSERT OR REPLACE INTO claims(chain_id, token, claimed_at) VALUES(?,?,?)',
                (chain_id, token, _now()))
            self.database.commit()
            row = self.database.execute('SELECT chain_id, url FROM chains WHERE chain_id=?',
                                        (chain_id,)).fetchone()
            return {'chain_id': row['chain_id'], 'url': row['url']}

    def release_claim(self, chain_id):
        with self.lock:
            self.database.execute('DELETE FROM claims WHERE chain_id=?', (chain_id,))
            self.database.commit()

    def open_chain_count(self):
        with self.lock:
            return self.database.execute(
                "SELECT COUNT(*) AS n FROM chains WHERE state='open' AND url IS NOT NULL"
            ).fetchone()['n']

    def claims_held(self):
        with self.lock:
            return self.database.execute('SELECT COUNT(*) AS n FROM claims').fetchone()['n'] > 0

    def has_pending_work(self):
        with self.lock:
            return self.open_chain_count() > 0 or self.claims_held()

    def add_budget(self, amount=1):
        with self.lock:
            used = int(self._get_meta('budget_used') or 0) + amount
            self._set_meta('budget_used', used)
            self.database.commit()
            return used

    def finish(self, budget_exhausted):
        """Reconcile overall walk status from the persisted chains."""
        with self.lock:
            chains = self.database.execute('SELECT state, kind, outlet_id, gen, reason FROM chains').fetchall()
            current = self.database.execute(
                'SELECT o.outlet_id, MAX(o.gen) AS gen FROM outlets o GROUP BY o.outlet_id').fetchall()
            current_keys = {(row['outlet_id'], row['gen']) for row in current}
            open_chains = [c for c in chains if c['state'] == 'open']
            failed_current = [c for c in chains
                              if c['state'] == 'failed' and c['kind'] == 'slots'
                              and (c['outlet_id'], c['gen']) in current_keys]
            root_failed = any(c['state'] == 'failed' and c['kind'] == 'root' for c in chains)
            if open_chains:
                status = 'paused'
                reason = ('page budget exhausted after ' + str(self._get_meta('budget_used'))
                          + ' fetches') if budget_exhausted else 'interrupted with open chains'
            elif root_failed or failed_current:
                status = 'failed'
                reason = 'one or more required chains failed'
            else:
                status = 'complete'
                reason = None
            self._set_meta('status', status)
            self._set_meta('status_reason', reason or '')
            self._set_meta('updated_at', _now())
            self.database.commit()
            return self.snapshot()

    # ------------------------------------------------------------- queries
    def snapshot(self):
        with self.lock:
            status = self._get_meta('status')
            reason = self._get_meta('status_reason')
            budget_used = int(self._get_meta('budget_used') or 0)
            chains = [dict(row) for row in self.database.execute(
                'SELECT chain_id, kind, outlet_id, gen, expected_version, url, state, reason '
                'FROM chains ORDER BY chain_id')]
            outlets = [dict(row) for row in self.database.execute(
                'SELECT o.outlet_id, o.gen, o.root_version, o.slot_version, o.state, o.reason, '
                '(SELECT COUNT(*) FROM slots s WHERE s.outlet_id=o.outlet_id AND s.gen=o.gen) AS slot_count '
                'FROM outlets o ORDER BY o.outlet_id, o.gen')]
            isolated = [dict(row) for row in self.database.execute(
                'SELECT chain_id, url, reason, observed_owner, observed_version FROM isolated_pages')]
            current = {}
            for outlet in outlets:
                if outlet['outlet_id'] not in current or outlet['gen'] > current[outlet['outlet_id']]['gen']:
                    current[outlet['outlet_id']] = outlet
            return {
                'status': status,
                'reason': reason or None,
                'root_url': self.root_url,
                'allowed_origin': self.allowed_origin,
                'max_pages': self.max_pages,
                'budget_used': budget_used,
                'chains': chains,
                'outlets': outlets,
                'current_outlets': list(current.values()),
                'isolated_pages': isolated,
            }

    def query_outlet(self, outlet_id):
        with self.lock:
            latest = self.database.execute(
                'SELECT * FROM outlets WHERE outlet_id=? ORDER BY gen DESC LIMIT 1',
                (outlet_id,)).fetchone()
            if latest is None:
                return None
            slots = [dict(row) for row in self.database.execute(
                'SELECT slot_id, slot_version, record_digest FROM slots '
                'WHERE outlet_id=? AND gen=? ORDER BY CAST(slot_id AS INTEGER), slot_id',
                (outlet_id, latest['gen']))]
            chain_id = self._child_chain_id(outlet_id, latest['gen'], latest['slot_version'])
            chain = self.database.execute(
                'SELECT state, reason, url FROM chains WHERE chain_id=?', (chain_id,)).fetchone()
            return {
                'outlet_id': outlet_id,
                'gen': latest['gen'],
                'root_version': latest['root_version'],
                'slot_version': latest['slot_version'],
                'state': latest['state'],
                'reason': latest['reason'],
                'slots': slots,
                'child_chain': dict(chain) if chain else None,
            }

    def close(self):
        with self.lock:
            self.database.close()
