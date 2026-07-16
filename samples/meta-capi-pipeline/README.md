# Meta CAPI Skill for Cortex Code

A Cortex Code skill that builds and manages Meta Conversions API (CAPI) pipelines in Snowflake.

## Features

- **Natural Language Triggers**: "set up Meta CAPI", "discover tables", "get recommendations"
- **Human-in-the-Loop**: All pipeline deployments require explicit approval
- **Auto-Discovery**: Scans your tables to find CAPI-compatible event sources
- **Meta AI Recommendations**: Fetches signal recommendations from Meta's Use Case API
- **CDC Pipelines**: Automatic change data capture from source tables
- **PII Hashing**: SHA256 hashing of emails/phones before transmission

## Installation

This skill lives in [Snowflake-Labs/sf-samples](https://github.com/Snowflake-Labs/sf-samples/tree/main/samples/meta-capi-pipeline).

### Option 1: Clone and symlink

```bash
# Clone the sf-samples repo anywhere
git clone https://github.com/Snowflake-Labs/sf-samples.git

# Symlink the skill into your Cortex Code skills directory
ln -s "$(pwd)/sf-samples/samples/meta-capi-pipeline" ~/.snowflake/cortex/skills/meta-capi-pipeline
```

### Option 2: Sparse checkout (skill only)

```bash
git clone --depth 1 --filter=blob:none --sparse https://github.com/Snowflake-Labs/sf-samples.git
cd sf-samples
git sparse-checkout set samples/meta-capi-pipeline
ln -s "$(pwd)/samples/meta-capi-pipeline" ~/.snowflake/cortex/skills/meta-capi-pipeline
```

### Verify Installation

In Cortex Code, run:
```
/skills
```

You should see `meta-capi-pipeline` listed.

## Usage

### Trigger Phrases

| Say this... | To do this... |
|-------------|---------------|
| "set up Meta CAPI" | Create pipeline infrastructure |
| "discover tables for Meta" | Find CAPI-compatible tables |
| "get signal recommendations" | Fetch Meta AI recommendations |
| "run CAPI pipeline" | Process pending events |
| "check CAPI status" | View pipeline health |
| "debug CAPI" | Troubleshoot failures |

### Example Workflow

```
You: set up Meta CAPI

Cortex Code: I'll create the Meta CAPI pipeline infrastructure.
Please provide:
- Pixel ID: [from Meta Events Manager]
- Access Token: [with ads_management permission]
...
```

## Prerequisites

Your use of Meta Conversions API (CAPI) is governed by your agreements with Meta.

Before using this skill, ensure you have:

- [ ] **Snowflake**: ACCOUNTADMIN or CREATE INTEGRATION privilege
- [ ] **Meta Pixel ID**: From [Meta Events Manager](https://business.facebook.com/events_manager)
- [ ] **Meta Access Token**: With `ads_management` permission
- [ ] **Warehouse**: For task execution (e.g., `COMPUTE_WH`)

## Skill Structure

```
meta-capi-pipeline/
├── SKILL.md                 # Main skill definition
├── README.md                # This file
├── references/
│   └── meta_events.md       # Meta standard events reference
└── sql/
    ├── 00_discovery/        # Table discovery & pipeline config
    ├── 01_setup/            # Schema & network access
    ├── 02_processing/       # UDTF & procedures
    ├── 03_monitoring/       # Health checks & alerts
    ├── 04_troubleshooting/  # Validation & error handling
    └── 05_recommendations/  # Meta Use Case API integration
```

## Objects Created

| Object | Type | Purpose |
|--------|------|---------|
| `META_CAPI_DB.PIPELINE` | Schema | Container for all objects |
| `META_CAPI_EVENTS` | Table | Event staging (PENDING → SENT) |
| `META_CAPI_LOG` | Table | Batch processing logs |
| `send_to_meta_capi` | UDTF | Calls Meta Graph API |
| `meta_capi_integration` | Integration | External network access |

## License

Snowflake Skills License. See the [LICENSE](./LICENSE) file for details.
