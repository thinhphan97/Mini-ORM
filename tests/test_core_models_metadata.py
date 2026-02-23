from __future__ import annotations

import unittest
from dataclasses import dataclass, field
from typing import Optional

from mini_orm.core.metadata import build_model_metadata
from mini_orm.core.models import (
    RelationSpec,
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
