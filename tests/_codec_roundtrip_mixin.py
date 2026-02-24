from __future__ import annotations

from typing import Any

from mini_orm import C


class CodecRoundtripMixin:
    codec_repo: Any
    codec_ticket_cls: type[Any]
    codec_closed_status: Any
    db: Any

    def test_enum_and_json_codec_roundtrip(self) -> None:
        with self.db.transaction():
            ticket = self.codec_repo.insert(
                self.codec_ticket_cls(
                    status=self.codec_closed_status,
                    payload={"priority": 2, "tags": ["bug"]},
                    tags=["bug", "urgent"],
                )
            )

        self.assertIsNotNone(ticket.id)

        loaded = self.codec_repo.get(ticket.id)
        self.assertIsNotNone(loaded)
        self.assertEqual(loaded.status, self.codec_closed_status)
        self.assertEqual(loaded.payload, {"priority": 2, "tags": ["bug"]})
        self.assertEqual(loaded.tags, ["bug", "urgent"])

        rows = self.codec_repo.list(where=C.eq("status", self.codec_closed_status))
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].id, ticket.id)

        with self.db.transaction():
            updated = self.codec_repo.update_where(
                {"payload": {"priority": 1}},
                where=C.eq("status", self.codec_closed_status),
            )
        self.assertEqual(updated, 1)
        refreshed = self.codec_repo.get(ticket.id)
        self.assertEqual(refreshed.payload, {"priority": 1})
        self.assertEqual(refreshed.tags, ["bug", "urgent"])
