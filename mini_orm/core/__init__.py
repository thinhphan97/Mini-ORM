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
from .validated_model import ValidatedModel, ValidationError
from .repositories.repository import RelatedResult, Repository, UnifiedRepository
from .repositories.repository_async import AsyncRepository, AsyncUnifiedRepository
from .repositories.repository_relations_async import AsyncRelatedResult
from .session import Session
from .session_async import AsyncSession
from .schemas.schema import (
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
from .vectors.vector_metrics import VectorMetric, VectorMetricInput, normalize_vector_metric
from .vectors.vector_policies import VectorIdPolicy
from .vectors.vector_repository import VectorRepository
from .vectors.vector_repository_async import AsyncVectorRepository
from .vectors.vector_codecs import (
    IdentityVectorPayloadCodec,
    JsonVectorPayloadCodec,
    VectorPayloadCodec,
)
from .vectors.vector_types import VectorRecord, VectorSearchResult

__all__ = [
    "C",
    "Condition",
    "ConditionGroup",
    "NotCondition",
    "OrderBy",
    "WhereExpression",
    "WhereInput",
    "DataclassModel",
    "ValidatedModel",
    "ValidationError",
    "RelationSpec",
    "RelationType",
    "Repository",
    "UnifiedRepository",
    "AsyncRepository",
    "AsyncUnifiedRepository",
    "RelatedResult",
    "AsyncRelatedResult",
    "Session",
    "AsyncSession",
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
