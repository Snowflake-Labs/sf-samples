# 05 — Getting and storing API keys (with CoCo)

Once CoCo has helped you identify a source (docs/03), you'll need your **own** credentials
for some of them. This page is how to get and store them safely. **Keys never go in git or
in a skill file** — they live in a git-ignored `.env`.

## Which sources need what

| Source | Key needed? | `.env` var |
|---|---|---|
| A — search interest | Entity lookup key (client itself often keyless) | `SEARCH_ENTITY_API_KEY` |
| B — trailer comments | Yes, if using the official platform API | `COMMENT_PLATFORM_API_KEY` |
| C — consumer research pageviews | No key; just a polite contact string | `PAGEVIEW_CONTACT` |
| D — box office | Licensed/official access credential | `BOXOFFICE_ACCESS_TOKEN` |
| E — metadata (static only) | Yes | `METADATA_API_KEY` |

## Ask CoCo to walk you through each one

> "I've identified Source A. Walk me through creating an account with that provider and
> generating the entity-lookup API key, tell me the free-tier limits and rate limits, and
> show me exactly what to paste into my `.env` — without putting the key anywhere in the
> repo."

Repeat per source. CoCo can also help you:
- test a key with a single minimal request,
- detect and back off from rate limits,
- rotate a key if it's ever exposed.

## Storing keys safely

1. `cp .env.example .env` (the copy is git-ignored).
2. Paste each real value next to its variable in `.env`.
3. Load them in a shell or script:
   ```bash
   set -a; source .env; set +a         # exports vars for local scripts
   ```
4. In Python, read via `os.environ["..."]` — never hardcode.

### Prefer secret storage for anything sensitive
CoCo supports a local secret store so values never appear in conversation. Ask:
> "Store my `COMMENT_PLATFORM_API_KEY` in the secret store and use it via `secret_env` when
> you run my ingest, so the value never shows up in the chat or in any file."

## Golden rules
- **Never** commit `.env`, `*.key`, `*.p8`, or `connections.toml` (`.gitignore` blocks them).
- **Never** paste a key into a skill file, a source dossier, or a prompt you'll commit.
- If a key is ever exposed, **rotate it immediately** with the provider.

Next: **`docs/06_model_overview.md`**.
