from __future__ import annotations

import json
import unittest
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional

from mini_orm.core.metadata import build_model_metadata
from mini_orm.core.models import (
    _infer_has_many_relations,
    _parse_relation_type,
    RelationSpec,
    RelationType,
    auto_pk_field,
    model_fields,
    model_relations,
    pk_fields,
    require_dataclass_model,
    row_to_model,
    table_name,
    to_dict,
)


@dataclass
class UserModel:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    email: str = ""
    age: Optional[int] = None


@dataclass
class CustomTableModel:
    __table__ = "custom_users"
    id: int = field(default=0, metadata={"pk": True})


@dataclass
class NoPkModel:
    email: str = ""


@dataclass
class MultiPkModel:
    id1: int = field(default=0, metadata={"pk": True})
    id2: int = field(default=0, metadata={"pk": True})


@dataclass
class CompanyModel:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    name: str = ""


@dataclass
class EmployeeModel:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    company_id: Optional[int] = None
    email: str = ""

    __relations__ = {
        "company": {
            "model": CompanyModel,
            "local_key": "company_id",
            "remote_key": "id",
            "type": "belongs_to",
        }
    }


@dataclass
class MetaAuthor:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    name: str = ""


@dataclass
class MetaPost:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    author_id: Optional[int] = field(
        default=None,
        metadata={
            "fk": (MetaAuthor, "id"),
            "relation": "author",
            "related_name": "posts",
        },
    )
    title: str = ""


@dataclass
class OverrideTeamModel:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    name: str = ""


@dataclass
class OverrideMemberModel:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    team_id: Optional[int] = field(
        default=None,
        metadata={"fk": (OverrideTeamModel, "id"), "related_name": "members"},
    )


class ProfileStatus(str, Enum):
    ACTIVE = "active"
    INACTIVE = "inactive"


@dataclass
class CodecModel:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    status: ProfileStatus = ProfileStatus.ACTIVE
    payload: dict[str, Any] = field(default_factory=dict)
    tags: list[str] = field(default_factory=list)
    custom_payload: Any = field(default_factory=dict, metadata={"codec": "json"})


class PlainObject:
    pass


class ModelsAndMetadataTests(unittest.TestCase):
    def test_require_dataclass_model_rejects_non_dataclass(self) -> None:
        with self.assertRaises(TypeError):
            require_dataclass_model(PlainObject)

    def test_table_name_resolves_default_and_override(self) -> None:
        self.assertEqual(table_name(UserModel), "usermodel")
        self.assertEqual(table_name(UserModel(email="a")), "usermodel")
        self.assertEqual(table_name(CustomTableModel), "custom_users")

    def test_model_fields_returns_declared_fields(self) -> None:
        names = [f.name for f in model_fields(UserModel)]
        self.assertEqual(names, ["id", "email", "age"])

    def test_pk_fields_and_auto_pk_field(self) -> None:
        pks = pk_fields(UserModel)
        self.assertEqual([f.name for f in pks], ["id"])
        self.assertEqual(auto_pk_field(UserModel).name, "id")
        self.assertIsNone(auto_pk_field(MultiPkModel))

    def test_pk_fields_raise_for_missing_pk(self) -> None:
        with self.assertRaises(ValueError):
            pk_fields(NoPkModel)

    def test_to_dict_and_row_to_model(self) -> None:
        user = UserModel(id=3, email="alice@example.com", age=20)
        self.assertEqual(
            to_dict(user),
            {"id": 3, "email": "alice@example.com", "age": 20},
        )

        mapped = row_to_model(
            UserModel, {"id": 7, "email": "bob@example.com", "age": 30}
        )
        self.assertEqual(mapped.id, 7)
        self.assertEqual(mapped.email, "bob@example.com")
        self.assertEqual(mapped.age, 30)

    def test_to_dict_and_row_to_model_support_enum_and_json_codec(self) -> None:
        obj = CodecModel(
            id=1,
            status=ProfileStatus.INACTIVE,
            payload={"score": 10},
            tags=["orm", "codec"],
            custom_payload={"flag": True},
        )
        serialized = to_dict(obj)

        self.assertEqual(serialized["status"], "inactive")
        self.assertEqual(json.loads(serialized["payload"]), {"score": 10})
        self.assertEqual(json.loads(serialized["tags"]), ["orm", "codec"])
        self.assertEqual(json.loads(serialized["custom_payload"]), {"flag": True})

        mapped = row_to_model(
            CodecModel,
            {
                "id": 2,
                "status": "active",
                "payload": '{"score": 20}',
                "tags": '["db", "orm"]',
                "custom_payload": '{"flag": false}',
            },
        )
        self.assertEqual(mapped.status, ProfileStatus.ACTIVE)
        self.assertEqual(mapped.payload, {"score": 20})
        self.assertEqual(mapped.tags, ["db", "orm"])
        self.assertEqual(mapped.custom_payload, {"flag": False})

    def test_build_model_metadata_success(self) -> None:
        metadata = build_model_metadata(UserModel)
        self.assertEqual(metadata.table, "usermodel")
        self.assertEqual(metadata.pk, "id")
        self.assertEqual(metadata.auto_pk, "id")
        self.assertEqual(metadata.columns, ["id", "email", "age"])
        self.assertEqual(metadata.writable_columns, ["email", "age"])
        self.assertEqual(metadata.relations, {})

    def test_model_relations_are_parsed_and_exposed_in_metadata(self) -> None:
        relations = model_relations(EmployeeModel)
        self.assertIn("company", relations)
        company_relation = relations["company"]
        self.assertIsInstance(company_relation, RelationSpec)
        self.assertFalse(company_relation.many)
        self.assertEqual(company_relation.local_key, "company_id")
        self.assertEqual(company_relation.remote_key, "id")
        self.assertIs(company_relation.model, CompanyModel)

        metadata = build_model_metadata(EmployeeModel)
        self.assertIn("company", metadata.relations)

    def test_model_relations_validate_invalid_keys(self) -> None:
        @dataclass
        class BrokenRelationModel:
            id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
            company_id: Optional[int] = None

            __relations__ = {
                "company": {
                    "model": CompanyModel,
                    "local_key": "missing_column",
                    "remote_key": "id",
                }
            }

        with self.assertRaises(ValueError):
            model_relations(BrokenRelationModel)

    def test_model_relations_accept_foreign_key_alias(self) -> None:
        @dataclass
        class AliasRelationModel:
            id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
            company_id: Optional[int] = None

            __relations__ = {
                "company": {
                    "model": CompanyModel,
                    "foreign_key": "company_id",
                    "remote_key": "id",
                    "type": "belongs_to",
                }
            }

        relations = model_relations(AliasRelationModel)
        self.assertEqual(relations["company"].local_key, "company_id")
        self.assertFalse(relations["company"].many)

    def test_model_relations_requires_mapping_type(self) -> None:
        @dataclass
        class InvalidRelationsTypeModel:
            id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
            __relations__ = []  # type: ignore[assignment]

        with self.assertRaises(TypeError):
            model_relations(InvalidRelationsTypeModel)

    def test_model_relations_validate_remote_key(self) -> None:
        @dataclass
        class InvalidRemoteKeyModel:
            id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
            company_id: Optional[int] = None

            __relations__ = {
                "company": {
                    "model": CompanyModel,
                    "local_key": "company_id",
                    "remote_key": "missing_remote",
                }
            }

        with self.assertRaises(ValueError):
            model_relations(InvalidRemoteKeyModel)

    def test_model_relations_validate_unsupported_relation_type(self) -> None:
        @dataclass
        class InvalidRelationTypeModel:
            id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
            company_id: Optional[int] = None

            __relations__ = {
                "company": {
                    "model": CompanyModel,
                    "local_key": "company_id",
                    "remote_key": "id",
                    "type": "many_to_many",
                }
            }

        with self.assertRaises(ValueError):
            model_relations(InvalidRelationTypeModel)

    def test_parse_relation_type_rejects_invalid_string(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"unsupported type 'many_to_many'",
        ):
            _parse_relation_type("many_to_many", relation_name="company")

    def test_model_relations_accept_relation_type_enum(self) -> None:
        @dataclass
        class EnumRelationModel:
            id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
            company_id: Optional[int] = None

            __relations__ = {
                "company": {
                    "model": CompanyModel,
                    "local_key": "company_id",
                    "remote_key": "id",
                    "type": RelationType.BELONGS_TO,
                }
            }

        relations = model_relations(EnumRelationModel)
        self.assertEqual(relations["company"].relation_type, RelationType.BELONGS_TO)
        self.assertFalse(relations["company"].many)

    def test_model_relations_are_inferred_from_fk_metadata(self) -> None:
        post_relations = model_relations(MetaPost)
        self.assertIn("author", post_relations)
        self.assertFalse(post_relations["author"].many)
        self.assertEqual(post_relations["author"].local_key, "author_id")
        self.assertEqual(post_relations["author"].remote_key, "id")
        self.assertIs(post_relations["author"].model, MetaAuthor)

        author_relations = model_relations(MetaAuthor)
        self.assertIn("posts", author_relations)
        self.assertTrue(author_relations["posts"].many)
        self.assertEqual(author_relations["posts"].local_key, "id")
        self.assertEqual(author_relations["posts"].remote_key, "author_id")
        self.assertIs(author_relations["posts"].model, MetaPost)

    def test_explicit_relations_override_equivalent_inferred_specs(self) -> None:
        inferred = _infer_has_many_relations(OverrideTeamModel)
        self.assertIn("members", inferred)
        self.assertIs(inferred["members"].model, OverrideMemberModel)

        orig = getattr(OverrideTeamModel, "__relations__", None)
        try:
            OverrideTeamModel.__relations__ = {
                "members": {
                    "model": OverrideMemberModel,
                    "local_key": "id",
                    "remote_key": "team_id",
                    "type": "has_many",
                }
            }

            relations = model_relations(OverrideTeamModel)
            self.assertEqual(list(relations.keys()), ["members"])
        finally:
            if orig is None:
                if hasattr(OverrideTeamModel, "__relations__"):
                    delattr(OverrideTeamModel, "__relations__")
            else:
                OverrideTeamModel.__relations__ = orig

    def test_model_relations_validate_model_must_be_dataclass(self) -> None:
        class NotDataclass:
            pass

        @dataclass
        class InvalidRelationModelType:
            id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
            company_id: Optional[int] = None

            __relations__ = {
                "company": {
                    "model": NotDataclass,
                    "local_key": "company_id",
                    "remote_key": "id",
                }
            }

        with self.assertRaises(TypeError):
            model_relations(InvalidRelationModelType)

    def test_build_model_metadata_requires_single_pk(self) -> None:
        with self.assertRaises(ValueError):
            build_model_metadata(MultiPkModel)


if __name__ == "__main__":
    unittest.main()
