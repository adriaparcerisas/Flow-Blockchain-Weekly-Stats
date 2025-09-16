# 🟩 Flow Ecosystem Dashboard

An interactive Streamlit dashboard tracking weekly activity across the Flow ecosystem, combining **Cadence (core)** and **EVM** data.  
It covers transactions, users, geography, NFTs, smart contracts, staking, and $FLOW price — with week-over-week deltas and long-term trends.

![screenshot-placeholder](./docs/screenshot.png)

---

## Table of Contents
- [Features](#features)
- [Pages](#pages)
  - [Overview](#overview)
  - [Transactions](#transactions)
  - [Staking](#staking)
  - [Users](#users)
  - [NFTs](#nfts)
  - [Smart Contracts](#smart-contracts)
  - [$FLOW Price & Tokens](#flow-price--tokens)
  - [Conclusions](#conclusions)
- [Data Freshness](#data-freshness)
- [Run Locally](#run-locally)
- [Deploy to Streamlit Cloud](#deploy-to-streamlit-cloud)
- [Notes & Methodology](#notes--methodology)

---

## Features
- **Weekly lens** with consistent ISO week truncation across metrics.
- **Last data week** banner so you always know the latest complete week available.
- **WoW deltas** and percentage changes on key KPIs.
- **CSV export** buttons for every table/visual.
- **Configurable cache TTL** (sidebar) to balance freshness vs cost.
- **Read-only Snowflake** access — no views/materializations required.

---

## Pages

### Overview
High-level snapshot for the latest completed week:
- Weekly transactions and WoW % change.
- Average tx fee in **FLOW** and **USD**.
- Key trends chart (weekly transactions).

### Transactions
- **Numbers (last 2 weeks by type):** succeeded vs failed with cumulative totals.
- **Succeeded vs Failed (weekly):** stacked/clustered bars to spot anomalies.
- **Cumulative transactions:** long-term growth curve.

### Staking
- **Staked vs Unstaked — Weekly:** gross and net volumes; totals vs weekly movements.
- **Unique stakers — Summary:** current vs previous week, Δ users and WoW %.
- **Cumulative** lines to observe compounding effects over time.

### Users
- **Users in Numbers (Totals & Week):** active users, new users, cumulative unique users, WoW deltas.
- **Users Over Time:** weekly evolution of active/new users.
- **Users by Region (Weekly Snapshot):** US, Europe, Asia, etc., inferred from activity hours.
- **Users by Region Over Time:** distribution of adoption across regions.

### NFTs
- **NFT Sales — Numbers (Week vs Prev):** sales, volume, buyers, active collections, WoW deltas.
- **NFT Sales Over Time:** includes Cadence marketplaces and EVM sources (Beezie, Mintify, OpenSea).

### Smart Contracts
- **Contracts — Numbers (Week vs Prev):** active contracts, new contracts, total contracts, WoW %.
- **Active Contracts Over Time:** Cadence + EVM merged view.
- **New Contracts Over Time:** Cadence vs EVM (COA vs non-COA) with cumulative unique.

### $FLOW Price & Tokens
- **$FLOW price (last week):** hourly price with derived % change and volatility.
- **Top tokens weekly movers:** average deviation over the last 7 days to surface out/under-performers.

### Conclusions
- Space to summarize highlights (momentum shifts, NFT spikes, staking swings, price moves) and add commentary.

---

## Data Freshness
- **Last data week** = `MAX(TRUNC(block_timestamp, 'WEEK'))` across Flow **core** + **core_evm** (extendable to NFTs if desired).
- All weekly charts exclude the current in-progress week to avoid partials.
- **Cache TTL** is adjustable in the sidebar (default 300s). Increase TTL for heavy pages (e.g., user geography).

---

## Notes & Methodology

* **Weekly windowing** uses ISO truncation; ensure all comparisons align to `TRUNC(...,'WEEK')`.
* **Users by region** are inferred from hourly activity patterns (proxy for timezone) — treat as indicative, not exact.
* **EVM + Cadence merges** de-duplicate when needed (e.g., by tx id/hash) to avoid double counting.
* For cost/perf, prefer **narrow SELECTs**, reasonable **WHERE** clauses, and a higher **cache TTL** on heavy pages.

---

*Maintained by Adrià Parcerisas — Flow Weekly Stats Dashboard.*
