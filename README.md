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
  automation_condition_type: "eager"  # Options: "none", "eager", "on_missing", "cron", "on_cron"
  # Optional: Link external observable assets as upstream dependencies
  has_observable_deps: false
  observable_dep_keys: []  # Example: ["raw_data.table1", "external_source.table2"]
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

## Observable Dependencies

The orchestration component supports linking external observable assets (non-dbt assets) as upstream dependencies to dbt models. This is useful when you have:
- External data ingestion processes that produce assets
- dbt models that consume this data
- A need to create proper lineage in the Dagster asset graph

### Configuration

```yaml
type: dbt_cloud_demo.defs.dbt_cloud_orchestration.component.DbtCloudOrchestrationComponent
attributes:
  demo_mode: false
  # ... other config ...
  has_observable_deps: true
  observable_dep_keys:
    - "raw_data.snowpipe_daily_position"
    - "external_source.customer_data"
```

### How It Works

1. Observable assets are specified using dot-notation (e.g., `"key1.key2.key3"` becomes `AssetKey(["key1", "key2", "key3"])`)
2. The component automatically adds these as upstream dependencies to dbt models that reference sources
3. This creates proper lineage: **Observable Asset → dbt Staging Models → dbt Mart Models**
4. Dagster's automation conditions will respect these dependencies when scheduling runs

### Example

If you have an observable asset representing a Snowpipe ingestion and a dbt staging model that reads from it:

```yaml
observable_dep_keys:
  - "Observable.observable_POC_NEXGEN_SNOWPIPE_DAILY_POSITION"
```

The staging model `stg_daily_position` will automatically have the observable asset as an upstream dependency, ensuring proper orchestration order.

### Complete Configuration Example

```yaml
type: dbt_cloud_demo.defs.dbt_cloud_orchestration.component.DbtCloudOrchestrationComponent
attributes:
  demo_mode: false
  account_id: "{{ env.DBT_CLOUD_ACCOUNT_ID }}"
  access_url: "{{ env.DBT_CLOUD_ACCESS_URL }}"
  token: "{{ env.DBT_CLOUD_TOKEN }}"
  project_id: "{{ env.DBT_CLOUD_PROJECT_ID }}"
  environment_id: "{{ env.DBT_CLOUD_ENVIRONMENT_ID }}"

  # Asset translation
  translation:
    group_name: "{{ node.fqn[1] if node.fqn|length > 1 else 'default' }}"

  # Automation conditions
  automation_condition_type: "eager"
  automation_sensor_minimum_interval_seconds: 5

  # Observable dependencies
  has_observable_deps: true
  observable_dep_keys:
    - "Observable.observable_POC_NEXGEN_SNOWPIPE_DAILY_POSITION"

  # Per-asset automation overrides
  automation_condition_overrides:
    - asset_keys: ["high_priority_model"]
      condition_type: "cron"
      cron_schedule: "0 */4 * * *"  # Every 4 hours

requirements:
  env:
    - DBT_CLOUD_ACCOUNT_ID
    - DBT_CLOUD_ACCESS_URL
    - DBT_CLOUD_TOKEN
    - DBT_CLOUD_PROJECT_ID
    - DBT_CLOUD_ENVIRONMENT_ID
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

## Known Issues

### dbt Cloud Timeout Configuration

#### Job Execution Timeout

The timeout for dbt Cloud job execution **is configurable**. By default, the component uses a 300-second (5-minute) timeout:

```python
yield from dbt_cloud.cli(args=["build"], context=context).wait(timeout=300)
```

You can adjust this in `component.py` line 300 if your dbt jobs require more time.

#### gRPC Startup Timeout

When using multiple dbt Cloud components or large dbt projects, you may encounter a startup timeout:

```
dagster._core.errors.DagsterUserCodeProcessError: Exception: Timed out waiting for gRPC server to start after 180s.
```

**Cause**: During `dagster dev` startup, each dbt Cloud component queries the dbt Cloud API to derive asset specs, which can exceed the default 180-second gRPC startup timeout.

**Solution**: Increase the startup timeout in `dagster.yaml`:

```yaml
code_servers:
  local_startup_timeout: 300  # Increase to 5 minutes (or higher as needed)
```

**Additional Optimizations**:
1. Optimize your dbt Cloud project to compile faster (fewer models, simpler dependencies)
2. Load components sequentially rather than in parallel
3. Use caching strategies to avoid repeated API calls during development

## Support

For issues or questions:
- [Dagster Slack Community](https://dagster.io/slack)
- [Dagster GitHub](https://github.com/dagster-io/dagster)
- [dbt Cloud Documentation](https://docs.getdbt.com/docs/cloud/about-cloud/dbt-cloud)
