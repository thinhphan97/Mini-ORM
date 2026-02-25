"""Public core API for query building, schema, and repository operations."""

from .conditions import C, Condition, ConditionGroup, NotCondition, OrderBy, WhereExpression
from .models import (
    DataclassModel,
    RelationSpec,
    RelationType,
    auto_pk_field,
    model_fields,
    model_relations,
    pk_fields,
    row_to_model,
    table_name,
    to_dict,
)
from .query_builder import WhereInput
from .repository import RelatedResult, Repository, UnifiedRepository
from .repository_async import AsyncRepository, AsyncUnifiedRepository
from .repository_relations_async import AsyncRelatedResult
from .schema import (
    IndexSpec,
    apply_schema,
    apply_schema_async,
    ensure_schema,
    ensure_schema_async,
    create_index_sql,
    create_indexes_sql,
    create_schema_sql,
    create_table_sql,
)
from .vector_metrics import VectorMetric, VectorMetricInput, normalize_vector_metric
from .vector_policies import VectorIdPolicy
from .vector_repository import VectorRepository
from .vector_repository_async import AsyncVectorRepository
from .vector_codecs import (
    IdentityVectorPayloadCodec,
    JsonVectorPayloadCodec,
    VectorPayloadCodec,
)
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
    "RelationSpec",
    "RelationType",
    "Repository",
    "UnifiedRepository",
    "AsyncRepository",
    "AsyncUnifiedRepository",
    "RelatedResult",
    "AsyncRelatedResult",
    "VectorMetric",
    "VectorMetricInput",
    "VectorIdPolicy",
    "VectorRecord",
    "VectorSearchResult",
    "VectorRepository",
    "AsyncVectorRepository",
    "VectorPayloadCodec",
    "IdentityVectorPayloadCodec",
    "JsonVectorPayloadCodec",
    "IndexSpec",
    "apply_schema",
    "apply_schema_async",
    "ensure_schema",
    "ensure_schema_async",
    "auto_pk_field",
    "create_index_sql",
    "create_indexes_sql",
    "create_schema_sql",
    "create_table_sql",
    "model_fields",
    "model_relations",
    "pk_fields",
    "row_to_model",
    "table_name",
    "to_dict",
    "normalize_vector_metric",
]
