"""Bounded-concurrency walker driven entirely by the persisted ledger.

Completeness is never derived from an in-memory todo list: each worker claims
an open chain from SQLite, fetches exactly one page, and lets the ledger decide
identity, generation membership and the next link.  A process restart simply
re-opens the ledger and resumes the chains that are still open.

A worker may temporarily find no claimable chain while peers are still fetching
pages that can open new child chains.  Workers park on a condition variable and
the run only ends when the persisted ledger itself reports no pending work, so
a momentarily empty claim table is never mistaken for walk completion.
"""

import threading
import uuid

from .ledger import Ledger
from .transport import fetch


class Walker:
    def __init__(self, state_dir, root_url=None, allowed_origin=None,
                 max_pages=1000, concurrency=2):
        self.ledger = Ledger(state_dir, root_url, allowed_origin, max_pages, concurrency)
        self.concurrency = self.ledger.concurrency
        self.max_pages = self.ledger.max_pages

    def run(self):
        """Fetch pages until no open chain remains or the page budget is spent.

        max_pages bounds this process run only; a resumed walk gets a fresh
        budget while every already-fetched page stays durable in the ledger.
        """
        self.ledger.update_run_config(self.max_pages, self.concurrency)
        condition = threading.Condition()
        remaining = self.max_pages
        parked = 0
        done = False

        def worker():
            nonlocal remaining, parked, done
            token = uuid.uuid4().hex
            while True:
                with condition:
                    if done or remaining <= 0:
                        return
                task = self.ledger.claim_next(token)
                if task is None:
                    with condition:
                        parked += 1
                        condition.notify_all()
                        should_exit = False
                        while True:
                            if done or remaining <= 0:
                                should_exit = True
                                break
                            if not self.ledger.has_pending_work():
                                # Durable truth, not an in-memory count: every
                                # chain is terminal and nobody holds a claim.
                                done = True
                                condition.notify_all()
                                should_exit = True
                                break
                            condition.wait(timeout=0.5)
                            break  # woken or timed out: retry claiming below
                        parked -= 1
                        condition.notify_all()
                        if should_exit:
                            return
                    continue
                with condition:
                    # The budget is spent only by an actual page fetch.
                    remaining -= 1
                chain_id, url = task['chain_id'], task['url']
                try:
                    status, _headers, payload = fetch(url, self.ledger.allowed_origin)
                    self.ledger.apply_response(chain_id, url, payload, status)
                except (ValueError, OSError) as error:
                    self.ledger.mark_chain_failed(chain_id, 'fetch failed: ' + str(error))
                finally:
                    self.ledger.release_claim(chain_id)
                    self.ledger.add_budget(1)
                    with condition:
                        condition.notify_all()

        threads = [threading.Thread(target=worker, name='walk-' + str(i), daemon=True)
                   for i in range(self.concurrency)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        exhausted = self.ledger.open_chain_count() > 0
        return self.ledger.finish(budget_exhausted=exhausted)

    def close(self):
        self.ledger.close()


def run_walk(state_dir, root_url, allowed_origin, max_pages=1000, concurrency=2):
    walker = Walker(state_dir, root_url, allowed_origin, max_pages, concurrency)
    try:
        return walker.run()
    finally:
        walker.close()


def resume_walk(state_dir, max_pages=None, concurrency=None):
    """Re-open an existing ledger and continue its still-open chains."""
    walker = Walker(state_dir)
    if max_pages is not None:
        walker.max_pages = max_pages
    if concurrency is not None:
        walker.concurrency = concurrency
    try:
        return walker.run()
    finally:
        walker.close()
