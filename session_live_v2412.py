"""Finite LIVE allocation. Historical accounting is read, never reset."""
import json
import math
import time

TOTALS_SQL = """SELECT
 COALESCE(SUM(CASE WHEN mode='live' AND
  (status IN ('completed','forced_unwind','manual_closed') OR
   (status='open_exposure' AND residual_cost_usd IS NOT NULL))
  THEN realized_pnl ELSE 0 END),0) AS pnl,
 SUM(CASE WHEN mode='live' AND mid_acquired>0 THEN 1 ELSE 0 END) AS buys,
 SUM(CASE WHEN mode='live' AND unwind_client_id IS NOT NULL THEN 1 ELSE 0 END) AS unwinds
 FROM live_attempts_v240"""


def totals(row):
    result = dict(pnl=float(row['pnl'] or 0), buys=int(row['buys'] or 0),
                  unwinds=int(row['unwinds'] or 0))
    if not math.isfinite(result['pnl']) or min(result['buys'], result['unwinds']) < 0:
        raise ValueError('Invalid journal totals')
    return result


class LiveSession:
    def __init__(self, policy, now_ms=None, monotonic=None):
        self.wall = now_ms or (lambda: int(time.time() * 1000))
        self.mono = monotonic or time.monotonic
        self.policy = json.loads(json.dumps(policy))
        p = self.policy
        if not isinstance(p.get('session_id'), str) or len(p['session_id']) != 32:
            raise ValueError('Invalid session identity')
        for key in ('created_ts_ms', 'expires_ts_ms', 'max_buys'):
            if type(p.get(key)) is not int:
                raise ValueError('Invalid session integer')
        limit = p.get('additional_loss_usd')
        if type(limit) not in (int, float) or not math.isfinite(limit) or not 0 < limit <= 5:
            raise ValueError('Explicit additional loss allocation must be in (0, 5]')
        if not 0 < p['expires_ts_ms'] - p['created_ts_ms'] <= 30 * 60 * 1000:
            raise ValueError('Session duration exceeds 30 minutes')
        if not 1 <= p['max_buys'] <= 10:
            raise ValueError('Session exceeds 10 buys')
        self.baseline = totals(p['baseline'])
        self.deadline = self.mono() + max(0, p['expires_ts_ms'] - self.wall()) / 1000

    def status(self, current):
        p = self.policy
        result = dict(required=True, session_id=p['session_id'],
                      additional_loss_usd=p['additional_loss_usd'],
                      baseline_realized_pnl=self.baseline['pnl'],
                      expires_ts_ms=p['expires_ts_ms'], max_buys=p['max_buys'],
                      pnl=None, buys=None, reason=None)
        if current is None:
            result['reason'] = 'session_accounting_unavailable'
            return result
        current = totals(current)
        result.update(pnl=current['pnl'] - self.baseline['pnl'],
                      buys=current['buys'] - self.baseline['buys'],
                      unwinds=current['unwinds'] - self.baseline['unwinds'])
        wall = self.wall()
        if wall < p['created_ts_ms'] - 1000:
            result['reason'] = 'session_clock_invalid'
        elif wall >= p['expires_ts_ms'] or self.mono() >= self.deadline:
            result['reason'] = 'session_expired'
        elif result['buys'] < 0 or result['unwinds'] < 0:
            result['reason'] = 'session_journal_regressed'
        elif result['unwinds'] > 0:
            result['reason'] = 'session_first_unwind'
        elif result['pnl'] <= -p['additional_loss_usd'] + 1e-9:
            result['reason'] = 'session_loss_limit'
        elif result['buys'] >= p['max_buys']:
            result['reason'] = 'session_buy_limit'
        return result
