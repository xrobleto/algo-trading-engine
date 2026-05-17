"""Horizon strategies — pure, backtestable signal logic.

Each strategy is a pure function of (MarketView, state). The same objects are
imported and called by both the live engine and the backtest harness, so there
is exactly one source of truth per strategy.
"""
