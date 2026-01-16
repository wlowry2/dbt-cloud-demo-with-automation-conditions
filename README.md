# dbt Cloud Demo - Dagster Integration

This project demonstrates both **observability** and **orchestration** use cases for dbt Cloud integration with Dagster.

## Overview

The project contains two custom Dagster components that showcase different patterns for working with dbt Cloud:

1. **dbt Cloud Observability Component** (`dbt_cloud_observability/`)
   - Loads dbt Cloud asset specifications into Dagster
   - Provides visibility into dbt Cloud assets in the Dagster Asset Graph
   - Monitors run history and materialization status via polling sensor
   - Useful for teams who run dbt Cloud independently but want visibility in Dagster

2. **dbt Cloud Orchestration Component** (`dbt_cloud_orchestration/`)
   - Triggers dbt Cloud jobs directly from Dagster
   - Schedules dbt runs based on upstream dependencies or cron schedules
   - Applies automation conditions for intelligent scheduling
   - Creates end-to-end data pipeline with dbt Cloud as a step

## Project Structure

```
dbt-cloud-demo/
├── src/dbt_cloud_demo/
│   ├── definitions.py              # Main definitions entry point
│   └── defs/
│       ├── dbt_cloud_observability/
│       │   ├── component.py        # Observability component logic
│       │   └── defs.yaml          # Component instance configuration
│       └── dbt_cloud_orchestration/
│           ├── component.py        # Orchestration component logic
│           └── defs.yaml          # Component instance configuration
├── pyproject.toml
└── README.md
```

## Demo Mode

Both components include a **demo mode** that allows you to run the project locally without actual dbt Cloud credentials. This is perfect for demonstrations, testing, and learning.

### Demo Mode Features

**Observability Component (Demo Mode):**
- Creates 5 mock dbt Cloud assets simulating a typical dbt project:
  - Staging models: `stg_customers`, `stg_orders`, `stg_payments`
  - Mart models: `customers`, `orders`
- Shows proper asset grouping and dependencies
- No external connections required

**Orchestration Component (Demo Mode):**
- Creates a complete data pipeline with 14 assets:
  - 3 raw data ingestion assets
  - 3 dbt staging transformation assets
  - 2 dbt mart assets
  - 1 downstream analytics asset
- Demonstrates automation conditions (eager execution)
- Shows realistic asset lineage and dependencies

## Getting Started

### Prerequisites

- Python 3.10+
- `uv` package manager (recommended) or `pip`

### Installation

1. Navigate to the project directory:
   ```bash
   cd dbt-cloud-demo
   ```

2. Install dependencies:

   **Option 1: Using uv (recommended)**
   ```bash
   uv sync
   ```

   **Option 2: Using pip**
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   pip install -e ".[dev]"
   ```

### Running in Demo Mode

The project is pre-configured to run in demo mode. Simply start the Dagster development server:

```bash
uv run dg dev
```

Or if using pip:

```bash
dg dev
```

Then open http://localhost:3000 to view the Dagster UI.

### Asset Lineage

**Observability Component Assets:**
```
dbt_cloud/staging/stg_customers  ─┐
dbt_cloud/staging/stg_orders     ─┼─→ dbt_cloud/marts/customers
dbt_cloud/staging/stg_payments   ─┤
                                  └─→ dbt_cloud/marts/orders
```

**Orchestration Component Assets:**
```
raw/customers  ─→ dbt_orchestrated/staging/stg_customers  ─┐
raw/orders     ─→ dbt_orchestrated/staging/stg_orders     ─┼─→ dbt_orchestrated/marts/customers  ─┐
raw/payments   ─→ dbt_orchestrated/staging/stg_payments   ─┤                                       ├─→ analytics/customer_lifetime_value
                                                            └─→ dbt_orchestrated/marts/orders      ─┘
```

## Connecting to Real dbt Cloud

To connect to an actual dbt Cloud account, update the component YAML files:

### For Observability:

Edit `src/dbt_cloud_demo/defs/dbt_cloud_observability/defs.yaml`:

```yaml
type: dbt_cloud_demo.defs.dbt_cloud_observability.component.DbtCloudObservabilityComponent
attributes:
  demo_mode: false
  account_id: "${DBT_CLOUD_ACCOUNT_ID}"
  access_url: "https://cloud.getdbt.com"
  token: "${DBT_CLOUD_TOKEN}"
  project_id: "${DBT_CLOUD_PROJECT_ID}"
  environment_id: "${DBT_CLOUD_ENVIRONMENT_ID}"
```

### For Orchestration:

Edit `src/dbt_cloud_demo/defs/dbt_cloud_orchestration/defs.yaml`:

```yaml
type: dbt_cloud_demo.defs.dbt_cloud_orchestration.component.DbtCloudOrchestrationComponent
attributes:
  demo_mode: false
  account_id: "${DBT_CLOUD_ACCOUNT_ID}"
  access_url: "https://cloud.getdbt.com"
  token: "${DBT_CLOUD_TOKEN}"
  project_id: "${DBT_CLOUD_PROJECT_ID}"
  environment_id: "${DBT_CLOUD_ENVIRONMENT_ID}"
  job_id: "${DBT_CLOUD_JOB_ID}"
```

### Set Environment Variables:

```bash
export DBT_CLOUD_ACCOUNT_ID="your_account_id"
export DBT_CLOUD_TOKEN="your_api_token"
export DBT_CLOUD_PROJECT_ID="your_project_id"
export DBT_CLOUD_ENVIRONMENT_ID="your_environment_id"
export DBT_CLOUD_JOB_ID="your_job_id"  # Only for orchestration
```

## Validation Commands

```bash
# Validate that all definitions load correctly
uv run dg check defs

# List all assets and sensors
uv run dg list defs

# Materialize a specific asset
uv run dg materialize <asset_key>

# Start the development server
uv run dg dev
```

## Use Cases

### Observability Pattern

**When to use:**
- You're already running dbt Cloud jobs independently
- You want visibility into dbt assets within Dagster
- You need to track dbt Cloud run history in Dagster
- You want to monitor dbt Cloud assets alongside other data assets

**Key features:**
- External asset representation
- Polling sensor for run history
- No changes to existing dbt Cloud workflows

### Orchestration Pattern

**When to use:**
- You want Dagster to trigger dbt Cloud jobs
- You need to coordinate dbt runs with upstream/downstream dependencies
- You want intelligent scheduling based on data availability
- You're building end-to-end pipelines orchestrated by Dagster

**Key features:**
- Materializable dbt Cloud assets
- Automation conditions (eager, cron, etc.)
- Direct job triggering from Dagster
- Full lineage tracking

## Component Architecture

Both components use Dagster's Component system with:

- **Pydantic Models** for parameter validation via class attributes
- **Class-based components** inheriting from `dg.Component`, `dg.Model`, and `dg.Resolvable`
- **Demo mode** for local testing without credentials
- **YAML configuration** for easy customization

## Learn More

- [dbt Cloud Integration Docs](https://docs.dagster.io/integrations/libraries/dbt/dbt-cloud)
- [Dagster Components Guide](https://docs.dagster.io/guides/build/components)
- [Creating Custom Components](https://docs.dagster.io/guides/build/components/creating-new-components)
- [Dagster Documentation](https://docs.dagster.io/)
- [Dagster University](https://courses.dagster.io/)
- [Dagster Slack Community](https://dagster.io/slack)

## Support

For issues or questions:
- [Dagster Slack Community](https://dagster.io/slack)
- [Dagster GitHub](https://github.com/dagster-io/dagster)
- [dbt Cloud Documentation](https://docs.getdbt.com/docs/cloud/about-cloud/dbt-cloud)
