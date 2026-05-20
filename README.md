## Disclaimer

This repository is shared for educational and reference purposes only. It is not financial advice! The strategy is no longer actively used, and any use, modification, or deployment is entirely at your own risk.


Live Intraday Ladder Pattern Trading Dashboard

Python-based trading research dashboard that uses Polygon’s REST API to fetch 1-minute OHLCV stock data and detect intraday ladder-pattern price movements. The system scans multiple tickers concurrently, evaluates rolling time windows, and filters signals using price movement, volume, monotonic ratio, efficiency, R² trend fit, and candle-direction consistency.

Implemented a weighted Nasdaq market-bias model using QQQ, SPY, mega-cap technology stocks, semiconductor stocks, and a broader ticker basket. Also added per-ticker day-bias scoring based on session return, recent lookback return, VWAP position, and relative strength versus the market. Built a local browser dashboard with live refresh, signal ranking, ticker search, captured-signal tracking, and optional email/Discord alerts.

Focus: Market-data pipeline, quantitative signal detection, real-time dashboard, trading research.
