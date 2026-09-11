# 01 — Install Cortex Code and connect it to Snowflake

Cortex Code (CoCo) is Snowflake's agentic desktop IDE. You'll use it to build and run
everything in this repo through conversation.

## 1. Install Cortex Code Desktop

1. Go to Snowflake's official documentation for Cortex Code and download the desktop app
   for your OS. https://www.snowflake.com/en/product/snowflake-coco/downloads/
2. Install and launch it.

> Tip: you can share this github with CoCo once
> it's running to give it the context of the project.

## 2. Make sure you have the Snowflake CLI

Cortex Code uses named Snowflake connections. The `snow` CLI is the easiest way to create
and test them.

```bash
# macOS (Homebrew)
brew install snowflake-cli
snow --version
```

For other platforms, follow the official Snowflake CLI install docs.

## 3. Add a connection to your account

You need: your **account identifier**, your **username**, and an auth method. **Key-pair
auth is strongly recommended** for agentic use because it doesn't trigger a browser popup
on every action.

```bash
snow connection add
# You'll be prompted for:
#   Connection name:      my_sandbox
#   Account:              <your_org>-<your_account>
#   User:                 <your_username>
#   Authenticator:        SNOWFLAKE_JWT        (key-pair)  — or  externalbrowser (SSO)
#   Private key file:     ~/.snowflake/keys/rsa_key.p8     (if using key-pair)
#   Role / Warehouse / Database / Schema: set to your sandbox once you have it (docs/02)
```

### Generating a key pair (if needed for your organizations login practices if you don't have one)

```bash
# private key (encrypted) + public key
openssl genrsa 2048 | openssl pkcs8 -topk8 -v2 aes-256-cbc -inform PEM -out rsa_key.p8
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
```

Give the **public** key to whoever administers your Snowflake user (or set it yourself if
you can):

```sql
ALTER USER <your_username> SET RSA_PUBLIC_KEY='<paste the public key body, no header/footer>';
```

Keep the **private** key local and never commit it (`.gitignore` already blocks `*.p8`).

## 4. Test the connection

```bash
snow connection test --connection my_sandbox
snow sql --connection my_sandbox -q "SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_ACCOUNT();"
```

## 5. Point Cortex Code at this folder

Open this repository folder in Cortex Code. It will auto-load `COCO.md`, which primes the
agent to act as your setup guide. Then open the chat and try:

> "Read COCO.md and docs/02. I don't have a sandbox database yet — help me draft the
> request to my data team."

## Connection cheatsheet

| Thing | Where it comes from |
|---|---|
| Account identifier | Snowsight → account menu, or your admin |
| Username | Your Snowflake login |
| Role | Your sandbox role (see `docs/02`) |
| Warehouse | A small warehouse you're allowed to use (e.g. an XS) |
| Auth | Key-pair (`SNOWFLAKE_JWT`) recommended; SSO (`externalbrowser`) works too |

Next: **`docs/02_request_a_sandbox.md`**.
