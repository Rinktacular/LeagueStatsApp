from typing import Optional, Iterable
import psycopg

class Ledger:
    def __init__(self, dsn: str):
        self.dsn = dsn

    def seen(self, match_id: str) -> bool:
        with psycopg.connect(self.dsn) as con, con.cursor() as cur:
            cur.execute("select 1 from seen_match_ids where match_id=%s", (match_id,))
            return cur.fetchone() is not None

    def mark_seen(self, match_id: str, region: str):
        with psycopg.connect(self.dsn) as con, con.cursor() as cur:
            cur.execute(
              "insert into seen_match_ids(match_id, region) values(%s,%s) on conflict do nothing",
              (match_id, region)
            )
            con.commit()

    def enqueue_matches(self, region: str, ids: Iterable[str]) -> int:
        inserted = 0
        with psycopg.connect(self.dsn) as con, con.cursor() as cur:
            for mid in ids:
                cur.execute(
                  "insert into match_queue(match_id, region) values(%s,%s) on conflict do nothing",
                  (mid, region)
                )
                inserted += cur.rowcount
            con.commit()
        return inserted

    def pop_next_match(self) -> Optional[tuple[str, str, int]]:
        with psycopg.connect(self.dsn) as con, con.cursor() as cur:
            cur.execute("""
              update match_queue
                 set picked_at = now(), status='processing'
               where id = (
                 select id from match_queue
                  where status='queued'
                  order by enqueued_at asc
                  limit 1
               )
              returning match_id, region, id
            """)
            row = cur.fetchone()
            return row if row else None

    def mark_done(self, queue_id: int):
        with psycopg.connect(self.dsn) as con, con.cursor() as cur:
            cur.execute("update match_queue set done_at=now(), status='done' where id=%s", (queue_id,))
            con.commit()
