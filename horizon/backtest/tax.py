"""US tax model for a taxable brokerage account (see docs/TAX_STUDY_PREREG.md).

Turns the fills and dividends recorded by `account_sim.simulate` into taxes:
  - FIFO lot relief (Alpaca's default cost-basis method);
  - wash sales: a loss is disallowed when the same symbol is bought within 30
    days before or after the sale; the disallowed loss is added to the basis
    of the replacement lot (no holding-period tacking — conservative);
  - annual short/long-term netting with loss carryforward (the $3,000
    ordinary-income offset is ignored — conservative);
  - dividends taxed in the year received: bond, T-bill and commodity-fund
    distributions at the ordinary rate, equity-ETF dividends at the LT rate;
  - full liquidation at the end, so a comparison with buy-and-hold is fair.

Accounting may look ahead (wash-sale windows need future buys); it never
feeds back into trading decisions.
"""

from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Dict, List, Tuple

import pandas as pd

ORDINARY_DISTRIBUTORS = {"BIL", "SHV", "TLT", "VGLT", "IEF", "DBC", "PDBC", "HYG", "LQD"}
WASH_DAYS = 30


@dataclass
class TaxRates:
    st: float = 0.32
    lt: float = 0.15

    @property
    def label(self) -> str:
        return f"ST {self.st:.0%} / LT {self.lt:.0%}"


@dataclass
class TaxReport:
    by_year: Dict[int, float]            # tax paid for each calendar year
    terminal: float                      # tax on full liquidation at the end
    st_realized: float                   # gross short-term gains realized (positive parts)
    lt_realized: float
    wash_disallowed: float               # total losses disallowed by the wash-sale rule

    @property
    def st_share(self) -> float:
        tot = self.st_realized + self.lt_realized
        return self.st_realized / tot if tot > 0 else 0.0


def _net(st: float, lt: float, carry: float) -> Tuple[float, float, float]:
    """Apply carryforward (ST first) then cross-net ST/LT. Returns taxable
    (st, lt) and the new carryforward (<= 0)."""
    st += carry
    if st < 0 < lt:
        lt += st; st = 0.0
    elif lt < 0 < st:
        st += lt; lt = 0.0
    new_carry = 0.0
    if st < 0 and lt <= 0:
        new_carry = st + lt; st = lt = 0.0
    elif lt < 0:
        new_carry = lt; lt = 0.0
    elif st < 0:
        new_carry = st; st = 0.0
    return st, lt, new_carry


def compute_taxes(trades: List[dict], dividends: List[dict],
                  final_marks: Dict[str, float], final_date: pd.Timestamp,
                  rates: TaxRates) -> TaxReport:
    """trades: [{date, symbol, side, shares, price}] in execution order
    (price = fill incl. slippage). dividends: [{date, symbol, amount}]."""
    # Every buy gets an id and a replacement capacity (shares). The wash-sale
    # rule is SHARE-MATCHED (IRC 1091): a loss on N shares is disallowed only
    # for as many shares as were bought within +/-30 days, and each purchased
    # share can serve as a replacement only once. (A first version disallowed
    # the whole loss whenever any buy fell in the window, so a $20 daily
    # re-pin wiped out a $500 loss — fixed before any selection was made.)
    buys: List[dict] = []
    buys_by_sym: Dict[str, List[dict]] = defaultdict(list)
    for i, t in enumerate(trades):
        if t["side"] == "buy" and float(t["shares"]) > 0:
            b = {"id": i, "date": pd.Timestamp(t["date"]), "cap": float(t["shares"]),
                 "adj": 0.0}
            buys.append(b); buys_by_sym[t["symbol"]].append(b)
    buy_of_trade = {b["id"]: b for b in buys}

    lots: Dict[str, deque] = defaultdict(deque)   # [shares, basis_ps, acq_date, buy_id]
    st_y: Dict[int, float] = defaultdict(float)
    lt_y: Dict[int, float] = defaultdict(float)
    div_tax: Dict[int, float] = defaultdict(float)
    st_gross = lt_gross = wash_total = 0.0

    for i, t in enumerate(trades):
        sym, d = t["symbol"], pd.Timestamp(t["date"])
        sh, px = float(t["shares"]), float(t["price"])
        if sh <= 0:
            continue
        if t["side"] == "buy":
            b = buy_of_trade[i]
            # Losses already washed onto this purchase raise its basis.
            lots[sym].append([sh, px + b["adj"] / sh, d, i])
            continue
        q = sh
        while q > 1e-12 and lots[sym]:
            lot = lots[sym][0]
            take = min(q, lot[0])
            loss_ps = lot[1] - px
            is_lt = (d - lot[2]).days > 365
            disallowed = 0.0
            if loss_ps > 0:
                need = take
                for b in buys_by_sym[sym]:
                    if need <= 1e-12:
                        break
                    if b["id"] == lot[3] or b["cap"] <= 1e-12:
                        continue
                    if abs((b["date"] - d).days) > WASH_DAYS:
                        continue
                    use = min(need, b["cap"])
                    b["cap"] -= use; need -= use
                    amt = use * loss_ps
                    disallowed += amt
                    # Add to the replacement shares' basis: to the open lot if
                    # it was already bought, else to the future purchase.
                    open_lot = next((L for L in lots[sym] if L[3] == b["id"]), None)
                    if open_lot is not None:
                        open_lot[1] += amt / open_lot[0]
                    elif b["date"] >= d:
                        b["adj"] += amt
                wash_total += disallowed
            gain = take * (px - lot[1]) + disallowed
            if abs(gain) > 1e-12:
                if is_lt:
                    lt_y[d.year] += gain; lt_gross += max(gain, 0.0)
                else:
                    st_y[d.year] += gain; st_gross += max(gain, 0.0)
            lot[0] -= take
            q -= take
            if lot[0] <= 1e-12:
                lots[sym].popleft()

    for dv in dividends:
        y = pd.Timestamp(dv["date"]).year
        rate = rates.st if dv["symbol"] in ORDINARY_DISTRIBUTORS else rates.lt
        div_tax[y] += max(float(dv["amount"]), 0.0) * rate

    by_year: Dict[int, float] = {}
    carry = 0.0
    years = sorted(set(st_y) | set(lt_y) | set(div_tax))
    for y in years:
        s, l, carry = _net(st_y.get(y, 0.0), lt_y.get(y, 0.0), carry)
        by_year[y] = s * rates.st + l * rates.lt + div_tax.get(y, 0.0)

    # Terminal liquidation of every open lot at the final marks.
    st_u = lt_u = 0.0
    for sym, dq in lots.items():
        px = final_marks.get(sym)
        if px is None:
            continue
        for sh, basis, acq, _bid in dq:
            g = sh * (px - basis)
            if (final_date - acq).days > 365:
                lt_u += g
            else:
                st_u += g
    s, l, _ = _net(st_u, lt_u, carry)
    terminal = s * rates.st + l * rates.lt
    return TaxReport(by_year, terminal, st_gross, lt_gross, wash_total)


def after_tax_curve(equity: pd.Series, report: TaxReport) -> pd.Series:
    """Pre-tax equity curve -> after-tax curve. Each year's tax is withdrawn
    from the account on its last trading day (pro-rata, so later returns
    scale down); the terminal liquidation tax comes off the final value."""
    f = pd.Series(1.0, index=equity.index)
    for y, tax in sorted(report.by_year.items()):
        in_year = equity.index[equity.index.year == y]
        if len(in_year) == 0 or tax == 0:
            continue
        ye = in_year[-1]
        f.loc[ye:] *= max(0.0, 1.0 - tax / float(equity.loc[ye]))
    out = equity * f
    out.iloc[-1] = out.iloc[-1] - report.terminal * float(f.iloc[-1])
    return out


def buy_and_hold_after_tax(tr_close: pd.Series, price: pd.Series,
                           dividends: pd.Series, rates: TaxRates,
                           ordinary: bool = False) -> pd.Series:
    """Buy-and-hold benchmark, same conventions: dividends reinvested and
    taxed yearly, one long-term sale at the end."""
    eq = tr_close / tr_close.iloc[0]
    shares = 1.0 / float(price.iloc[0])
    f = pd.Series(1.0, index=eq.index)
    div_rate = rates.st if ordinary else rates.lt
    reinvested = 0.0
    for y in sorted(set(eq.index.year)):
        idx = eq.index[eq.index.year == y]
        dv = dividends.reindex(idx).fillna(0.0)
        # dividend cash per $1 of starting value, scaled by reinvested shares
        units = eq.loc[idx] / price.reindex(idx).ffill()
        paid = float((dv * units).sum())
        reinvested += paid * float(f.loc[idx[-1]])
        f.loc[idx[-1]:] *= max(0.0, 1.0 - paid * div_rate / float(eq.loc[idx[-1]]))
    out = eq * f
    basis = 1.0 + reinvested            # reinvested dividends add to basis
    gain = float(out.iloc[-1]) - basis
    out.iloc[-1] -= max(gain, 0.0) * rates.lt
    return out
