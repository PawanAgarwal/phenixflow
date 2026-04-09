# 200-Week Moving Average Stock Screener

> "If all you ever did was buy high-quality stocks on the 200-week moving average, you would beat the S&P 500 by a large margin. The problem is that few human beings have that kind of discipline."

This screener automates the discipline. It scans S&P 500, NASDAQ-100, and other high-quality stocks to find those currently trading at or near their 200-week simple moving average.

## Setup

```bash
cd apps/stock-screener
npm install
```

## Usage

```bash
# Screen all stocks (S&P 500 + NASDAQ-100, ~560 tickers)
node index.js

# Screen only S&P 500
node index.js --index sp500

# Screen only NASDAQ-100
node index.js --index nasdaq100

# Custom symbols
node index.js --symbols AAPL,MSFT,GOOG,AMZN,META

# Adjust threshold (default: within 5% of 200w SMA)
node index.js --threshold 3       # Stricter: within 3%
node index.js --threshold 10      # Wider: within 10%

# Adjust quality filter (default: min score 5/13)
node index.js --quality 7         # Higher quality bar
node index.js --quality 0         # Show all matches
node index.js --no-quality        # Skip quality check (faster)

# JSON output for piping to other tools
node index.js --json
```

## How It Works

### Phase 1: 200-Week SMA Screen
For each stock, fetches ~5 years of weekly price data from Yahoo Finance and:
- Calculates the 200-week Simple Moving Average
- Measures % distance from current price to the 200w SMA
- Computes 50-week SMA for trend context
- Identifies momentum shifts (stocks recently crossing below the 200w SMA)

### Phase 2: Quality Filter
Enriches results with fundamentals to filter out value traps:
- **Market cap** (mega/large/mid)
- **Profitability** (EPS, operating margins)
- **Growth** (revenue growth YoY)
- **Balance sheet** (debt-to-equity)
- **Returns** (ROE)

Quality score ranges from 0-13. Default minimum is 5.

## Output

The screener shows:
- Current price vs 200w SMA with % distance
- 50-week trend direction
- 52-week high/low context
- Quality metrics (P/E, margins, ROE, growth, debt)
- Categorized summary (on MA, near MA, approaching)
- Top picks ranked by quality + proximity
