# Cloudflare R2 Logpush to Cortex Agent

Build an end-to-end pipeline that ingests Cloudflare HTTP request logs from R2 into Snowflake and exposes them through a natural-language queryable Cortex Agent.

## What You Get

- **Raw table** — VARIANT storage of Cloudflare HTTP request JSON
- **Parsed view** — ~75 typed columns (timestamps, geo, cache, bot, WAF, origin, Workers)
- **Semantic view** — 28 facts, 42 dimensions, 16 pre-built metrics with AI hints
- **Cortex Agent** — ask questions in plain English about traffic, errors, bots, cache, security, and more
- **Optional scheduled task** — weekly refresh from R2

## Prerequisites

- Snowflake account with ACCOUNTADMIN (or CREATE DATABASE + CREATE AGENT privileges)
- Cloudflare Logpush configured to push HTTP request logs to an R2 bucket
- R2 API credentials (access key + secret key)

## Usage

This sample is designed as a **Cortex Code skill**. Install the skill and invoke it — it will walk you through configuration, infrastructure creation, data loading, semantic view setup, and agent creation step by step.

See `SKILL.md` for the full workflow.
