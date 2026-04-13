#!/usr/bin/env python3
"""Portfolio SMA55 Stop-Loss Risk Analysis using Polygon.io"""

import requests
import pandas as pd
import numpy as np
import time
import json
from datetime import datetime, timedelta

POLYGON_API_KEY = "eskzZ5O4QQyYpS5TsA3pe5WMeepeaxmW"
ACCOUNT_EQUITY = 833761.23

holdings = [
    {"ticker":"NVDA","shares":1000.35,"total_return_usd":51641.87},
    {"ticker":"AMD","shares":308.85,"total_return_usd":26132.00},
    {"ticker":"META","shares":100.44,"total_return_usd":1353.06},
    {"ticker":"MSFT","shares":145.12,"total_return_usd":2533.04},
    {"ticker":"PLTR","shares":330.00,"total_return_usd":30602.60},
    {"ticker":"UNH","shares":184.48,"total_return_usd":-3292.22},
    {"ticker":"ANET","shares":325.00,"total_return_usd":-3483.50},
    {"ticker":"GOOGL","shares":142.20,"total_return_usd":29077.54},
    {"ticker":"ASML","shares":30.18,"total_return_usd":19439.89},
    {"ticker":"TSM","shares":111.33,"total_return_usd":15512.80},
    {"ticker":"ALAB","shares":300.00,"total_return_usd":-16067.15},
    {"ticker":"CRWV","shares":445.00,"total_return_usd":-10033.64},
    {"ticker":"MU","shares":80.24,"total_return_usd":18336.96},
    {"ticker":"CRM","shares":151.20,"total_return_usd":-6123.03},
    {"ticker":"AMZN","shares":120.67,"total_return_usd":12316.68},
    {"ticker":"NVTS","shares":2650.00,"total_return_usd":-838.97},
    {"ticker":"VRT","shares":92.34,"total_return_usd":11422.34},
    {"ticker":"AAPL","shares":86.30,"total_return_usd":2343.26},
    {"ticker":"IONQ","shares":600.00,"total_return_usd":-4070.72},
    {"ticker":"AEHR","shares":500.00,"total_return_usd":13800.00},
    {"ticker":"RGTI","shares":1100.00,"total_return_usd":572.15},
    {"ticker":"OKLO","shares":300.00,"total_return_usd":853.75},
    {"ticker":"CEG","shares":50.13,"total_return_usd":-4338.79},
    {"ticker":"DFEN","shares":200.91,"total_return_usd":3982.93},
    {"ticker":"SOFI","shares":800.00,"total_return_usd":-425.58},
    {"ticker":"APLD","shares":500.00,"total_return_usd":8900.55},
    {"ticker":"NBIS","shares":110.00,"total_return_usd":3080.05},
    {"ticker":"SHOP","shares":100.00,"total_return_usd":6825.49},
    {"ticker":"ET","shares":622.31,"total_return_usd":7560.10},
    {"ticker":"ADBE","shares":39.46,"total_return_usd":-3520.88},
    {"ticker":"AMZU","shares":350.00,"total_return_usd":-922.12},
    {"ticker":"AVGO","shares":30.00,"total_return_usd":368.30},
    {"ticker":"TSLA","shares":24.00,"total_return_usd":1822.95},
    {"ticker":"INTC","shares":200.00,"total_return_usd":2712.98},
    {"ticker":"SMCI","shares":400.00,"total_return_usd":-4699.88},
    {"ticker":"ORCL","shares":55.11,"total_return_usd":-3721.79},
    {"ticker":"IREN","shares":200.00,"total_return_usd":-210.00},
    {"ticker":"IBIT","shares":200.00,"total_return_usd":-2558.50},
    {"ticker":"HOOD","shares":100.00,"total_return_usd":-541.50},
    {"ticker":"SLV","shares":100.00,"total_return_usd":-1247.03},
    {"ticker":"VST","shares":40.07,"total_return_usd":-1113.56},
    {"ticker":"ASTS","shares":67.44,"total_return_usd":5256.98},
    {"ticker":"QBTS","shares":350.00,"total_return_usd":-659.25},
    {"ticker":"RKLB","shares":80.00,"total_return_usd":2953.30},
    {"ticker":"CVX","shares":25.24,"total_return_usd":856.43},
    {"ticker":"TEM","shares":100.00,"total_return_usd":-5369.50},
    {"ticker":"VLO","shares":20.11,"total_return_usd":1099.22},
    {"ticker":"ZG","shares":100.00,"total_return_usd":310.00},
    {"ticker":"OUST","shares":200.00,"total_return_usd":-899.38},
    {"ticker":"BMNR","shares":200.00,"total_return_usd":-134.02},
    {"ticker":"PANW","shares":25.00,"total_return_usd":-1115.85},
    {"ticker":"NOK","shares":501.45,"total_return_usd":147.38},
    {"ticker":"COIN","shares":20.00,"total_return_usd":773.37},
    {"ticker":"MP","shares":75.00,"total_return_usd":-229.50},
    {"ticker":"SYM","shares":75.00,"total_return_usd":-680.00},
    {"ticker":"CRWD","shares":9.56,"total_return_usd":1251.52},
    {"ticker":"UBER","shares":50.00,"total_return_usd":-794.00},
    {"ticker":"QUBT","shares":500.00,"total_return_usd":-3767.50},
    {"ticker":"DKNG","shares":150.00,"total_return_usd":183.82},
    {"ticker":"SMR","shares":300.00,"total_return_usd":-2425.66},
    {"ticker":"SNOW","shares":20.00,"total_return_usd":-509.82},
    {"ticker":"VIST","shares":50.00,"total_return_usd":1719.50},
    {"ticker":"MLI","shares":30.50,"total_return_usd":1213.88},
    {"ticker":"LULU","shares":20.00,"total_return_usd":9.00},
    {"ticker":"QCOM","shares":25.00,"total_return_usd":-211.07},
    {"ticker":"CRCL","shares":25.00,"total_return_usd":-72.25},
    {"ticker":"NVT","shares":25.05,"total_return_usd":515.25},
    {"ticker":"KTOS","shares":35.00,"total_return_usd":-547.59},
    {"ticker":"SERV","shares":300.00,"total_return_usd":-1018.00},
    {"ticker":"RDDT","shares":20.00,"total_return_usd":1680.66},
    {"ticker":"NOW","shares":25.00,"total_return_usd":-370.30},
    {"ticker":"MRVL","shares":30.10,"total_return_usd":-490.81},
    {"ticker":"CRDO","shares":25.00,"total_return_usd":-837.50},
    {"ticker":"AFRM","shares":50.00,"total_return_usd":-2412.00},
    {"ticker":"TQQQ","shares":50.00,"total_return_usd":-77.50},
    {"ticker":"SE","shares":27.60,"total_return_usd":479.49},
    {"ticker":"RBLX","shares":37.00,"total_return_usd":339.56},
    {"ticker":"HIVE","shares":1000.00,"total_return_usd":-3550.00},
    {"ticker":"CELH","shares":50.00,"total_return_usd":573.04},
    {"ticker":"V","shares":6.30,"total_return_usd":115.47},
    {"ticker":"BBAI","shares":500.00,"total_return_usd":5.02},
    {"ticker":"COPX","shares":25.00,"total_return_usd":-299.12},
    {"ticker":"RIVN","shares":100.00,"total_return_usd":327.00},
    {"ticker":"TTD","shares":65.00,"total_return_usd":-4442.74},
    {"ticker":"ARM","shares":10.00,"total_return_usd":149.00},
    {"ticker":"SOUN","shares":175.00,"total_return_usd":-764.30},
    {"ticker":"PYPL","shares":25.06,"total_return_usd":-1016.40},
    {"ticker":"ZS","shares":7.00,"total_return_usd":88.83},
    {"ticker":"CLSK","shares":100.00,"total_return_usd":-1237.00},
    {"ticker":"MARA","shares":100.00,"total_return_usd":-1358.00},
]

# Also fetch QQQ for beta calculation
QQQ_TICKER = "QQQ"

def fetch_daily_bars(ticker, start_date, end_date, max_retries=3):
    """Fetch daily OHLCV bars from Polygon."""
    url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start_date}/{end_date}"
    params = {
        "adjusted": "true",
        "sort": "asc",
        "limit": 5000,
        "apiKey": POLYGON_API_KEY,
    }
    for attempt in range(max_retries):
        try:
            r = requests.get(url, params=params, timeout=30)
            if r.status_code == 429:
                wait = 2 ** attempt * 5
                print(f"  Rate limited on {ticker}, waiting {wait}s...")
                time.sleep(wait)
                continue
            r.raise_for_status()
            data = r.json()
            if data.get("resultsCount", 0) == 0:
                return None
            return data["results"]
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
            else:
                print(f"  ERROR fetching {ticker}: {e}")
                return None
    return None

def fetch_last_trade(ticker, max_retries=3):
    """Fetch last trade price from Polygon."""
    url = f"https://api.polygon.io/v2/last/trade/{ticker}"
    params = {"apiKey": POLYGON_API_KEY}
    for attempt in range(max_retries):
        try:
            r = requests.get(url, params=params, timeout=15)
            if r.status_code == 429:
                time.sleep(2 ** attempt * 5)
                continue
            r.raise_for_status()
            data = r.json()
            if data.get("results") and data["results"].get("p"):
                return data["results"]["p"]
            return None
        except:
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
            else:
                return None
    return None

def main():
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=300)).strftime("%Y-%m-%d")

    all_tickers = list(set([h["ticker"] for h in holdings] + [QQQ_TICKER]))
    print(f"Fetching data for {len(all_tickers)} tickers...")

    # Fetch all daily bars
    ticker_bars = {}
    for i, ticker in enumerate(all_tickers):
        print(f"  [{i+1}/{len(all_tickers)}] {ticker}...", end=" ", flush=True)
        bars = fetch_daily_bars(ticker, start_date, end_date)
        if bars:
            ticker_bars[ticker] = bars
            print(f"{len(bars)} bars")
        else:
            print("NO DATA")
        # Small delay to avoid rate limits (free tier: 5/min)
        if (i + 1) % 5 == 0:
            time.sleep(12)  # Polygon free tier is 5 requests/min

    # Also fetch last trades for current price
    print("\nFetching last trade prices...")
    last_trades = {}
    for i, ticker in enumerate([h["ticker"] for h in holdings]):
        lt = fetch_last_trade(ticker)
        if lt:
            last_trades[ticker] = lt
        if (i + 1) % 5 == 0:
            time.sleep(12)
        if (i + 1) % 10 == 0:
            print(f"  [{i+1}/{len(holdings)}] done...")

    # Build QQQ returns for beta
    qqq_bars = ticker_bars.get(QQQ_TICKER, [])
    qqq_closes = [b["c"] for b in qqq_bars] if qqq_bars else []
    qqq_returns_20 = []
    if len(qqq_closes) >= 21:
        qqq_returns_20 = [
            (qqq_closes[i] - qqq_closes[i-1]) / qqq_closes[i-1]
            for i in range(len(qqq_closes)-20, len(qqq_closes))
        ]

    # Process each holding
    results = []
    insufficient_history = []

    for h in holdings:
        ticker = h["ticker"]
        shares = h["shares"]
        total_return = h["total_return_usd"]

        bars = ticker_bars.get(ticker, [])
        closes = [b["c"] for b in bars] if bars else []

        # Current price: prefer last trade, fallback to latest close
        current_price = last_trades.get(ticker)
        if current_price is None and closes:
            current_price = closes[-1]
        if current_price is None:
            print(f"  SKIP {ticker}: no price data")
            insufficient_history.append(ticker)
            continue

        # SMA55
        has_sma55 = len(closes) >= 55
        if has_sma55:
            sma55 = np.mean(closes[-55:])
        else:
            sma55 = np.nan
            insufficient_history.append(ticker)

        # % to SMA55
        if has_sma55:
            pct_to_sma55_signed = (current_price - sma55) / current_price
            pct_drop_to_sma55 = max(0, pct_to_sma55_signed)
            stop_loss_usd = max(0, (current_price - sma55) * shares)
        else:
            pct_to_sma55_signed = np.nan
            pct_drop_to_sma55 = np.nan
            stop_loss_usd = np.nan

        stop_loss_pct_equity = stop_loss_usd / ACCOUNT_EQUITY if not np.isnan(stop_loss_usd) else np.nan

        # Cost basis
        avg_cost = current_price - (total_return / shares)
        cost_basis = avg_cost * shares
        unrealized_return_pct = (total_return / cost_basis) if cost_basis != 0 else 0

        # Market value
        market_value = current_price * shares

        # Weight in portfolio (approximate, will normalize after)
        weight = market_value  # will compute pct later

        # Volatility 20d
        vol_20d = np.nan
        if len(closes) >= 21:
            rets = [(closes[i] - closes[i-1]) / closes[i-1] for i in range(len(closes)-20, len(closes))]
            vol_20d = np.std(rets) * np.sqrt(252)

        # Beta 20d vs QQQ
        beta_20d = np.nan
        if len(closes) >= 21 and len(qqq_returns_20) == 20:
            stock_rets = [(closes[i] - closes[i-1]) / closes[i-1] for i in range(len(closes)-20, len(closes))]
            if len(stock_rets) == 20:
                cov = np.cov(stock_rets, qqq_returns_20)[0][1]
                var_qqq = np.var(qqq_returns_20)
                beta_20d = cov / var_qqq if var_qqq > 0 else np.nan

        # Risk score
        risk_score = np.nan
        if not np.isnan(stop_loss_usd) and not np.isnan(beta_20d):
            risk_score = stop_loss_usd * (1 + max(0, beta_20d))

        results.append({
            "ticker": ticker,
            "shares": shares,
            "current_price": round(current_price, 2),
            "sma55": round(sma55, 2) if not np.isnan(sma55) else np.nan,
            "pct_to_sma55_signed": round(pct_to_sma55_signed * 100, 2) if not np.isnan(pct_to_sma55_signed) else np.nan,
            "pct_drop_to_sma55": round(pct_drop_to_sma55 * 100, 2) if not np.isnan(pct_drop_to_sma55) else np.nan,
            "stop_loss_usd": round(stop_loss_usd, 2) if not np.isnan(stop_loss_usd) else np.nan,
            "stop_loss_pct_equity": round(stop_loss_pct_equity * 100, 3) if not np.isnan(stop_loss_pct_equity) else np.nan,
            "avg_cost": round(avg_cost, 2),
            "cost_basis": round(cost_basis, 2),
            "market_value": round(market_value, 2),
            "total_return_usd": total_return,
            "unrealized_return_pct": round(unrealized_return_pct * 100, 2),
            "vol_20d": round(vol_20d * 100, 2) if not np.isnan(vol_20d) else np.nan,
            "beta_20d": round(beta_20d, 3) if not np.isnan(beta_20d) else np.nan,
            "risk_score": round(risk_score, 2) if not np.isnan(risk_score) else np.nan,
            "bars_available": len(closes),
        })

    df = pd.DataFrame(results)

    # Portfolio weight
    total_mv = df["market_value"].sum()
    df["weight_pct"] = (df["market_value"] / total_mv * 100).round(2)

    # Portfolio totals
    total_stop_loss = df["stop_loss_usd"].sum()
    total_stop_loss_pct = total_stop_loss / ACCOUNT_EQUITY * 100

    print(f"\n{'='*60}")
    print(f"PORTFOLIO SMA55 STOP-LOSS ANALYSIS")
    print(f"{'='*60}")
    print(f"Account Equity:       ${ACCOUNT_EQUITY:>14,.2f}")
    print(f"Total Market Value:   ${total_mv:>14,.2f}")
    print(f"Total Stop-Loss $:    ${total_stop_loss:>14,.2f}")
    print(f"Stop-Loss % Equity:   {total_stop_loss_pct:>13.2f}%")
    print(f"Holdings Analyzed:    {len(df)}")
    print(f"Insufficient History: {insufficient_history}")

    # Sort and save CSV
    df_sorted = df.sort_values("stop_loss_usd", ascending=False)
    csv_path = "G:/My Drive/Algo_Trading/outputs/portfolio_sma55_analysis.csv"
    df_sorted.to_csv(csv_path, index=False)
    print(f"\nCSV saved to: {csv_path}")

    # Build markdown summary
    md = []
    md.append("# Portfolio SMA55 Stop-Loss Risk Analysis")
    md.append(f"\n**Date:** {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    md.append(f"\n**Account Equity:** ${ACCOUNT_EQUITY:,.2f}")
    md.append(f"\n**Total Market Value:** ${total_mv:,.2f}")
    md.append(f"\n**Total Simultaneous Stop-Loss (all hit SMA55):** ${total_stop_loss:,.2f} ({total_stop_loss_pct:.2f}% of equity)")
    md.append(f"\n**Holdings Analyzed:** {len(df)} | **Insufficient History:** {', '.join(insufficient_history) if insufficient_history else 'None'}")

    # Top 15 by stop_loss_usd
    md.append("\n---\n## Top 15 Positions by Stop-Loss $ (largest absolute drawdown to SMA55)")
    md.append("\n| Rank | Ticker | Shares | Price | SMA55 | % Above SMA55 | Stop-Loss $ | % of Equity | Weight% |")
    md.append("|------|--------|--------|-------|-------|---------------|-------------|-------------|---------|")
    top_stop = df_sorted.head(15)
    for rank, (_, r) in enumerate(top_stop.iterrows(), 1):
        md.append(f"| {rank} | **{r['ticker']}** | {r['shares']:,.2f} | ${r['current_price']:,.2f} | ${r['sma55']:,.2f} | {r['pct_to_sma55_signed']:.1f}% | ${r['stop_loss_usd']:,.0f} | {r['stop_loss_pct_equity']:.2f}% | {r['weight_pct']:.1f}% |")

    # Top 15 by stop_loss_pct_equity
    md.append("\n---\n## Top 15 by Stop-Loss % of Equity")
    md.append("\n| Rank | Ticker | Stop-Loss $ | % of Equity | Market Value | % Above SMA55 |")
    md.append("|------|--------|-------------|-------------|--------------|---------------|")
    top_pct = df.sort_values("stop_loss_pct_equity", ascending=False).head(15)
    for rank, (_, r) in enumerate(top_pct.iterrows(), 1):
        md.append(f"| {rank} | **{r['ticker']}** | ${r['stop_loss_usd']:,.0f} | {r['stop_loss_pct_equity']:.2f}% | ${r['market_value']:,.0f} | {r['pct_to_sma55_signed']:.1f}% |")

    # Below SMA55
    below_sma = df[df["pct_to_sma55_signed"] < 0].sort_values("pct_to_sma55_signed")
    md.append(f"\n---\n## Holdings BELOW SMA55 (stop already triggered) — {len(below_sma)} positions")
    md.append("\n| Ticker | Price | SMA55 | % Below SMA55 | Unrealized P&L | Market Value |")
    md.append("|--------|-------|-------|---------------|----------------|--------------|")
    for _, r in below_sma.iterrows():
        md.append(f"| **{r['ticker']}** | ${r['current_price']:,.2f} | ${r['sma55']:,.2f} | {r['pct_to_sma55_signed']:.1f}% | ${r['total_return_usd']:+,.0f} | ${r['market_value']:,.0f} |")

    # Top 15 by risk_score
    md.append("\n---\n## Top 15 by Risk Score (stop_loss_usd × (1 + beta_20d))")
    md.append("\n| Rank | Ticker | Risk Score | Stop-Loss $ | Beta (20d) | Vol (20d ann.) | Weight% |")
    md.append("|------|--------|------------|-------------|------------|----------------|---------|")
    top_risk = df.dropna(subset=["risk_score"]).sort_values("risk_score", ascending=False).head(15)
    for rank, (_, r) in enumerate(top_risk.iterrows(), 1):
        md.append(f"| {rank} | **{r['ticker']}** | {r['risk_score']:,.0f} | ${r['stop_loss_usd']:,.0f} | {r['beta_20d']:.2f} | {r['vol_20d']:.1f}% | {r['weight_pct']:.1f}% |")

    # "Winning names are no longer safe" analysis
    winners = df[df["total_return_usd"] > 5000].sort_values("stop_loss_usd", ascending=False)
    md.append("\n---\n## 'Winning Names Are No Longer Safe' — Big Winners with Largest Stop-Loss Exposure")
    md.append("\nThese positions have large unrealized gains AND large stop-loss exposure to SMA55:")
    md.append("\n| Ticker | Unrealized P&L | Return % | Stop-Loss $ | % of Equity | % Above SMA55 | Weight% |")
    md.append("|--------|----------------|----------|-------------|-------------|---------------|---------|")
    for _, r in winners.iterrows():
        md.append(f"| **{r['ticker']}** | ${r['total_return_usd']:+,.0f} | {r['unrealized_return_pct']:.1f}% | ${r['stop_loss_usd']:,.0f} | {r['stop_loss_pct_equity']:.2f}% | {r['pct_to_sma55_signed']:.1f}% | {r['weight_pct']:.1f}% |")

    # Positions most dangerous because far above SMA55
    far_above = df[df["pct_to_sma55_signed"] > 0].sort_values("pct_drop_to_sma55", ascending=False).head(15)
    md.append("\n---\n## Most Extended Above SMA55 (largest potential % drop)")
    md.append("\n| Ticker | Price | SMA55 | % Above SMA55 | Stop-Loss $ | Market Value |")
    md.append("|--------|-------|-------|---------------|-------------|--------------|")
    for _, r in far_above.iterrows():
        md.append(f"| **{r['ticker']}** | ${r['current_price']:,.2f} | ${r['sma55']:,.2f} | {r['pct_to_sma55_signed']:.1f}% | ${r['stop_loss_usd']:,.0f} | ${r['market_value']:,.0f} |")

    # Tier classification
    md.append("\n---\n## Risk Tiers")
    md.append("\n### Tier 1 — CRITICAL (stop_loss > 2% of equity)")
    t1 = df[df["stop_loss_pct_equity"] > 2].sort_values("stop_loss_pct_equity", ascending=False)
    for _, r in t1.iterrows():
        md.append(f"- **{r['ticker']}**: ${r['stop_loss_usd']:,.0f} stop-loss ({r['stop_loss_pct_equity']:.2f}% of equity), {r['pct_to_sma55_signed']:.1f}% above SMA55")

    md.append("\n### Tier 2 — HIGH (stop_loss 1-2% of equity)")
    t2 = df[(df["stop_loss_pct_equity"] >= 1) & (df["stop_loss_pct_equity"] <= 2)].sort_values("stop_loss_pct_equity", ascending=False)
    for _, r in t2.iterrows():
        md.append(f"- **{r['ticker']}**: ${r['stop_loss_usd']:,.0f} stop-loss ({r['stop_loss_pct_equity']:.2f}% of equity)")

    md.append("\n### Tier 3 — MODERATE (stop_loss 0.5-1% of equity)")
    t3 = df[(df["stop_loss_pct_equity"] >= 0.5) & (df["stop_loss_pct_equity"] < 1)].sort_values("stop_loss_pct_equity", ascending=False)
    for _, r in t3.iterrows():
        md.append(f"- **{r['ticker']}**: ${r['stop_loss_usd']:,.0f} stop-loss ({r['stop_loss_pct_equity']:.2f}% of equity)")

    # Conclusions
    md.append("\n---\n## Key Conclusions")
    md.append(f"""
1. **Simultaneous SMA55 stop scenario** would cost **${total_stop_loss:,.0f}** ({total_stop_loss_pct:.1f}% of ${ACCOUNT_EQUITY:,.0f} equity).

2. **"Winning names are no longer safe"**: Your biggest winners by P&L (see table above) also carry the largest absolute stop-loss exposure. In a regime break, these names fall furthest because they're furthest above support.

3. **{len(below_sma)} positions already below SMA55** — these have already "triggered" in this framework and are trading without the 55-day trend as support. Consider whether these should be cut or have tighter trailing stops.

4. **Concentration risk**: The top 5 stop-loss positions alone account for a significant portion of total portfolio risk. Size management on winners is the primary lever.

5. **High-beta names** amplify the stop-loss risk — when QQQ sells off, high-beta positions will move to SMA55 faster than low-beta ones.
""")

    md_path = "G:/My Drive/Algo_Trading/outputs/portfolio_sma55_summary.md"
    with open(md_path, "w") as f:
        f.write("\n".join(md))
    print(f"Summary saved to: {md_path}")

    return df

if __name__ == "__main__":
    df = main()
