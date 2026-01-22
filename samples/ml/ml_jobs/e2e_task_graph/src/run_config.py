import os

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from snowflake.core.task.context import TaskContext
from snowflake.snowpark import Session
from constants import DAG_STAGE

ARTIFACT_DIR = "run_artifacts"

@dataclass(frozen=True)
class RunConfig:
    run_id: str
    dataset_name: str
    model_name: str
    metric_name: str
    metric_threshold: float

    @property
    def artifact_dir(self) -> str:
        return os.path.join(DAG_STAGE, ARTIFACT_DIR, self.run_id)  # noqa: F821

    @classmethod
    def from_task_context(cls, ctx: TaskContext, **kwargs: Any) -> "RunConfig":
        run_schedule = ctx.get_current_task_graph_original_schedule()
        run_id = "v" + (
            run_schedule.strftime("%Y%m%d_%H%M%S")
            if isinstance(run_schedule, datetime)
            else str(run_schedule)
        )
        run_config = dict(run_id=run_id)

        graph_config = ctx.get_task_graph_config()
        merged = run_config | graph_config | kwargs

        # Get expected fields from RunConfig
        expected_fields = set(cls.__annotations__)

        # Find unexpected keys
        unexpected_keys = [key for key in merged.keys() if key not in expected_fields]
        for key in unexpected_keys:
            print(f"Warning: Unexpected config key '{key}' will be ignored")

        filtered = {k: v for k, v in merged.items() if k in expected_fields}
        return cls(**filtered)

    @classmethod
    def from_session(cls, session: Session) -> "RunConfig":
        ctx = TaskContext(session)
        return cls.from_task_context(ctx)

