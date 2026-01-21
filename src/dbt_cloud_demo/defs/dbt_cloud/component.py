"""dbt Cloud components for observability and orchestration.

This module provides:
- DbtCloudObservabilityComponent: For viewing dbt Cloud assets in Dagster
- DbtCloudOrchestrationComponent: For orchestrating dbt runs with automation conditions
"""

from collections.abc import Mapping
from pathlib import Path
from typing import Annotated, Any, Optional

import dagster as dg
from dagster.components.utils.translation import TranslationFn, TranslationFnResolver
from dagster_dbt import (
    DagsterDbtTranslator,
    DbtCliResource,
    DbtCloudCredentials,
    DbtCloudWorkspace,
    DbtProject,
    build_dbt_cloud_polling_sensor,
    dbt_assets,
    dbt_cloud_assets,
    load_dbt_cloud_asset_specs,
)
from pydantic import BaseModel


class DbtCloudObservabilityComponent(dg.Component, dg.Model, dg.Resolvable):
    """Component for dbt Cloud observability.

    In real mode, this component:
    - Loads asset specifications from dbt Cloud
    - Creates external assets representing dbt Cloud models
    - Provides a polling sensor to sync run history
    """

    account_id: int
    access_url: str = "https://cloud.getdbt.com"
    token: str = ""
    project_id: int
    environment_id: int

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        """Build real dbt Cloud observability definitions."""
        # Validate required parameters
        required_params = {
            "account_id": self.account_id,
            "token": self.token,
            "project_id": self.project_id,
            "environment_id": self.environment_id
        }
        missing = [k for k, v in required_params.items() if not v]
        if missing:
            raise ValueError(f"Missing required parameters for real mode: {missing}")

        # Create credentials and workspace
        credentials = DbtCloudCredentials(
            account_id=int(self.account_id),
            token=dg.EnvVar("DBT_CLOUD_TOKEN") if self.token == "${DBT_CLOUD_TOKEN}" else self.token,
            access_url=self.access_url
        )

        workspace = DbtCloudWorkspace(
            credentials=credentials,
            project_id=int(self.project_id),
            environment_id=int(self.environment_id)
        )

        # Load asset specs from dbt Cloud
        asset_specs = load_dbt_cloud_asset_specs(workspace=workspace)

        # Build polling sensor
        dbt_cloud_sensor = build_dbt_cloud_polling_sensor(
            workspace=workspace,
        )

        return dg.Definitions(
            assets=asset_specs,
            sensors=[dbt_cloud_sensor]
        )


class AutomationConditionOverride(BaseModel):
    """Configuration for overriding automation condition on specific assets."""

    asset_keys: list[str]  # List of asset key names (e.g., ["customers", "stg_orders"])
    condition_type: str  # Options: "none", "eager", "on_missing", "cron", "on_cron"
    cron_schedule: Optional[str] = None  # Required if condition_type is "cron" or "on_cron" (uses UTC)


class _DbtCloudComponentTranslator(DagsterDbtTranslator):
    """Wrapper translator that applies a translation function to asset specs and automation condition."""

    def __init__(
        self,
        translation_fn: Optional[TranslationFn[Mapping[str, Any]]] = None,
        automation_condition: Optional[dg.AutomationCondition] = None,
        automation_condition_overrides: Optional[list[AutomationConditionOverride]] = None,
    ):
        self._translation_fn = translation_fn
        self._automation_condition = automation_condition
        self._automation_condition_overrides = automation_condition_overrides or []
        super().__init__()

    def get_asset_spec(self, manifest: Mapping[str, Any], unique_id: str, project) -> dg.AssetSpec:
        """Get asset spec, applying translation function if provided."""
        base_spec = super().get_asset_spec(manifest, unique_id, project)

        if self._translation_fn is None:
            return base_spec

        # Extract dbt node properties from manifest
        dbt_resource_props = manifest["nodes"].get(unique_id) or manifest["sources"].get(unique_id)

        return self._translation_fn(base_spec, dbt_resource_props)

    def get_automation_condition(self, dbt_resource_props: Mapping[str, Any]) -> Optional[dg.AutomationCondition]:
        """Return the automation condition for dbt asset, checking for overrides first."""
        # Get the asset name from dbt resource properties
        asset_name = dbt_resource_props.get("name", "")

        # Check if this asset has an override
        for override in self._automation_condition_overrides:
            if asset_name in override.asset_keys:
                # Build and return the override condition
                return self._build_condition_from_override(override)

        # No override found, return default automation condition
        return self._automation_condition

    def _build_condition_from_override(self, override: AutomationConditionOverride) -> Optional[dg.AutomationCondition]:
        """Build an automation condition from an override configuration."""
        if override.condition_type == "none":
            return None
        elif override.condition_type == "eager":
            return dg.AutomationCondition.eager()
        elif override.condition_type == "on_missing":
            return dg.AutomationCondition.on_missing()
        elif override.condition_type in ["cron", "on_cron"]:
            if not override.cron_schedule:
                raise ValueError(f"cron_schedule is required for condition_type '{override.condition_type}'")
            return dg.AutomationCondition.on_cron(
                cron_schedule=override.cron_schedule
            )
        else:
            raise ValueError(f"Unknown condition_type in override: {override.condition_type}")


class DbtCloudOrchestrationComponent(dg.Component, dg.Model, dg.Resolvable):
    """Component for dbt Cloud orchestration with demo mode support.

    In production mode (demo_mode=False):
    - Creates materializable dbt Cloud assets
    - Triggers dbt Cloud jobs from Dagster
    - Applies automation conditions for scheduling
    - Provides a polling sensor to monitor job execution

    In demo mode (demo_mode=True):
    - Uses a bundled demo dbt project with DuckDB
    - Executes dbt locally without requiring dbt Cloud credentials
    - Demonstrates typical dbt patterns (staging, marts, dependencies)
    - Skips cloud-specific features (sensors, cloud workspace)

    """

    # Demo mode configuration
    demo_mode: bool = True  # When True, uses local DuckDB instead of dbt Cloud

    # dbt Cloud credentials (not required in demo mode)
    account_id: int = 0
    access_url: str = "https://cloud.getdbt.com"
    token: str = ""
    project_id: int = 0
    environment_id: int = 0
    translation: Annotated[
        Optional[TranslationFn[Mapping[str, Any]]],
        TranslationFnResolver(template_vars_for_translation_fn=lambda data: {"node": data}),
    ] = None

    # Automation condition configuration
    automation_condition_type: str = "none"  # Options: "none", "eager", "on_missing", "cron", "on_cron"
    cron_schedule: str | None = None  # Required if automation_condition_type is "cron" or "on_cron" (uses UTC)
    automation_sensor_minimum_interval_seconds: int = 5  # How often to evaluate automation conditions (minimum 5 seconds)
    automation_condition_overrides: list[AutomationConditionOverride] = []  # Override automation for specific assets

    # Observable asset dependency configuration
    has_observable_deps: bool = False  # Flag to indicate external/observable asset dependencies
    observable_dep_keys: list[str] = []  # Asset keys of observable dependencies (e.g., ["my_external_asset"])

    def _get_demo_project_path(self) -> Path:
        """Get the path to the bundled demo dbt project."""
        return Path(__file__).parent / "demo_dbt_project"

    def _build_automation_condition(self) -> dg.AutomationCondition | None:
        """Build automation condition based on configuration."""
        if self.automation_condition_type == "none":
            return None
        elif self.automation_condition_type == "eager":
            return dg.AutomationCondition.eager()
        elif self.automation_condition_type == "on_missing":
            return dg.AutomationCondition.on_missing()
        elif self.automation_condition_type == "cron":
            if not self.cron_schedule:
                raise ValueError("cron_schedule is required when automation_condition_type is 'cron'")
            return dg.AutomationCondition.on_cron(
                cron_schedule=self.cron_schedule
            )
        elif self.automation_condition_type == "on_cron":
            if not self.cron_schedule:
                raise ValueError("cron_schedule is required when automation_condition_type is 'on_cron'")
            return dg.AutomationCondition.on_cron(
                cron_schedule=self.cron_schedule
            )
        else:
            raise ValueError(f"Unknown automation_condition_type: {self.automation_condition_type}")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        """Build dbt orchestration definitions (cloud or demo mode)."""
        if self.demo_mode:
            return self._build_demo_defs(context)
        else:
            return self._build_cloud_defs(context)

    def _build_cloud_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        """Build real dbt Cloud orchestration definitions."""

        # Create credentials and workspace
        credentials = DbtCloudCredentials(
            account_id=int(self.account_id),
            token=dg.EnvVar("DBT_CLOUD_TOKEN") if self.token == "${DBT_CLOUD_TOKEN}" else self.token,
            access_url=self.access_url
        )

        workspace = DbtCloudWorkspace(
            credentials=credentials,
            project_id=int(self.project_id),
            environment_id=int(self.environment_id)
        )

        # Build automation condition
        automation_condition = self._build_automation_condition()

        # Create translator with translation function, automation condition, and overrides
        translator = _DbtCloudComponentTranslator(
            translation_fn=self.translation,
            automation_condition=automation_condition,
            automation_condition_overrides=self.automation_condition_overrides
        )

        # Build observable asset dependencies
        deps = None
        if self.has_observable_deps and self.observable_dep_keys:
            # Convert string keys to AssetKey objects
            deps = [dg.AssetKey.from_user_string(key) for key in self.observable_dep_keys]

        # Create dbt Cloud assets that can be materialized
        @dbt_cloud_assets(
            workspace=workspace,
            dagster_dbt_translator=translator,
            name="dbt_cloud_orchestrated_assets",
            deps=deps,
        )
        def dbt_cloud_orchestrated_assets(context: dg.AssetExecutionContext, dbt_cloud: DbtCloudWorkspace):
            """Materializable dbt Cloud assets triggered by Dagster."""
            yield from dbt_cloud.cli(args=["build"], context=context).wait(timeout=300)

        # Build polling sensor to monitor job execution
        dbt_cloud_sensor = build_dbt_cloud_polling_sensor(
            workspace=workspace,
        )

        # Add automation sensor if automation condition is enabled
        sensors = [dbt_cloud_sensor]
        if automation_condition is not None:
            automation_sensor = dg.AutomationConditionSensorDefinition(
                name="dbt_cloud_automation_sensor",
                target="*",
                minimum_interval_seconds=self.automation_sensor_minimum_interval_seconds,
            )
            sensors.append(automation_sensor)

        return dg.Definitions(
            assets=[dbt_cloud_orchestrated_assets],
            resources={"dbt_cloud": workspace},
            sensors=sensors
        )

    def _build_demo_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        """Build demo mode definitions using local DuckDB."""

        # Create DbtProject for local execution
        demo_project_path = self._get_demo_project_path()
        dbt_project = DbtProject(
            project_dir=demo_project_path,
            target="dev"
        )
        dbt_project.prepare_if_dev()

        # Build automation condition
        automation_condition = self._build_automation_condition()

        # Create translator with translation function, automation condition, and overrides
        translator = _DbtCloudComponentTranslator(
            translation_fn=self.translation,
            automation_condition=automation_condition,
            automation_condition_overrides=self.automation_condition_overrides
        )

        # Build observable asset dependencies (for deps parameter if supported)
        deps = None
        if self.has_observable_deps and self.observable_dep_keys:
            deps = [dg.AssetKey.from_user_string(key) for key in self.observable_dep_keys]

        # Create local dbt assets
        # Automation condition is applied via the translator's get_automation_condition() method
        if deps is not None:
            @dbt_assets(
                manifest=dbt_project.manifest_path,
                dagster_dbt_translator=translator,
                name="dbt_demo_assets",
                project=dbt_project,
                deps=deps,
            )
            def dbt_demo_assets(context: dg.AssetExecutionContext, dbt: DbtCliResource):
                """Demo dbt assets running locally with DuckDB."""
                yield from dbt.cli(["build"], context=context).stream()
        else:
            @dbt_assets(
                manifest=dbt_project.manifest_path,
                dagster_dbt_translator=translator,
                name="dbt_demo_assets",
                project=dbt_project,
            )
            def dbt_demo_assets(context: dg.AssetExecutionContext, dbt: DbtCliResource):
                """Demo dbt assets running locally with DuckDB."""
                yield from dbt.cli(["build"], context=context).stream()

        # Create DbtCliResource for local execution
        dbt_resource = DbtCliResource(project_dir=dbt_project)

        # Create automation sensor with custom tick interval if automation is enabled
        sensors = []
        if automation_condition is not None:
            automation_sensor = dg.AutomationConditionSensorDefinition(
                name="dbt_demo_automation_sensor",
                target="*",  # Apply to all assets
                minimum_interval_seconds=self.automation_sensor_minimum_interval_seconds,
            )
            sensors.append(automation_sensor)

        return dg.Definitions(
            assets=[dbt_demo_assets],
            resources={"dbt": dbt_resource},
            sensors=sensors,
        )
