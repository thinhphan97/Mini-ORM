"""Public core API for query building, schema, and repository operations."""

from .conditions import C, Condition, ConditionGroup, NotCondition, OrderBy, WhereExpression
from .models import (
    DataclassModel,
    auto_pk_field,
    model_fields,
    pk_fields,
    row_to_model,
    table_name,
    to_dict,
)
from .query_builder import WhereInput
from .repository import Repository
from .schema import (
    IndexSpec,
    apply_schema,
    create_index_sql,
    create_indexes_sql,
    create_schema_sql,
    create_table_sql,
)
from .vector_metrics import VectorMetric, VectorMetricInput, normalize_vector_metric
from .vector_policies import VectorIdPolicy
from .vector_repository import VectorRepository
from .vector_types import VectorRecord, VectorSearchResult

__all__ = [
    "C",
    "Condition",
    "ConditionGroup",
    "NotCondition",
    "OrderBy",
    "WhereExpression",
    "WhereInput",
    "DataclassModel",
    "Repository",
    "VectorMetric",
    "VectorMetricInput",
    "VectorIdPolicy",
    "VectorRecord",
    "VectorSearchResult",
    "VectorRepository",
    "IndexSpec",
    "apply_schema",
    "auto_pk_field",
    "create_index_sql",
    "create_indexes_sql",
    "create_schema_sql",
    "create_table_sql",
    "model_fields",
    "pk_fields",
    "row_to_model",
    "table_name",
    "to_dict",
    "normalize_vector_metric",
]
