from typing import Any, ClassVar, Collection, Mapping, Callable
from collections.abc import Sequence
from airflow.sdk.bases.decorator import DecoratedOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk.definitions._internal.types import SET_DURING_EXECUTION
from airflow.utils.context import context_merge
from airflow.utils.operator_helpers import determine_kwargs
from airflow.sdk.definitions.context import Context
from airflow.sdk.bases.decorator import TaskDecorator, task_decorator_factory
import warnings
# Internal operator that combines Airflow's DecoratedOperator and SQLExecuteQueryOperator
class _SQLDecoratedOperator(DecoratedOperator, SQLExecuteQueryOperator):
    # Merge template fields and renderers from both parent classes
    template_fields: Sequence[str] = (*DecoratedOperator.template_fields, *SQLExecuteQueryOperator.template_fields)
    template_fields_renderers: ClassVar[dict[str, str]] = {
        **DecoratedOperator.template_fields_renderers,
        **SQLExecuteQueryOperator.template_fields_renderers,
    }

    # Custom operator name for identification
    custom_operator_name: str = "@task.sql"
    # Always overwrite runtime information after execution
    overwrite_rtif_after_execution: bool = True

    def __init__(
        self,
        *,
        python_callable: Callable,
        op_args: Collection[Any] | None = None,
        op_kwargs: Mapping[str, Any] | None = None,
        **kwargs,
    ) -> None:
        # Warn if multiple_outputs is set, as it's not supported
        if kwargs.pop("multiple_outputs", None):
            warnings.warn(
                f"`multiple_outputs=True` is not supported in {self.custom_operator_name} tasks. Ignoring.",
                UserWarning,
                stacklevel=3,
            )

        # Initialize parent classes, set sql to SET_DURING_EXECUTION
        super().__init__(
            python_callable=python_callable,
            op_args=op_args,
            op_kwargs=op_kwargs,
            sql=SET_DURING_EXECUTION,
            multiple_outputs=False,
            **kwargs,
        )

    def execute(self, context: Context) -> Any:
        # Merge context with operator keyword arguments
        context_merge(context, self.op_kwargs)
        # Determine arguments for the python_callable
        kwargs = determine_kwargs(self.python_callable, self.op_args, context)

        # Call the decorated function to get the SQL string
        self.sql = self.python_callable(*self.op_args, **kwargs)

        # Ensure the returned SQL is a non-empty string
        if not isinstance(self.sql, str) or self.sql.strip() == "":
            raise TypeError("The returned value from the TaskFlow callable must be a non-empty string.")

        # Render templates in the task instance
        context["ti"].render_templates()

        # Execute the SQL using the parent class's execute method
        return super().execute(context)
    
# This function creates a task decorator for SQL tasks    
def sql_task(
    python_callable: Callable | None = None,
    **kwargs,
) -> TaskDecorator:
    # Create a task decorator for SQL tasks using the provided callable and additional keyword arguments.
    return task_decorator_factory(
        python_callable=python_callable,
        decorated_operator_class=_SQLDecoratedOperator,
        **kwargs
    )