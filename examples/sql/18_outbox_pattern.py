"""Outbox pattern demo with transaction-scoped session."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

from mini_orm import C, Database, OrderBy, Session, SQLiteDialect


@dataclass
class OrderRow:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    customer_email: str = ""
    total_cents: int = 0
    status: str = "created"


@dataclass
class OutboxMessage:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    aggregate_type: str = ""
    aggregate_id: str = ""
    event_type: str = ""
    payload: dict[str, Any] = field(default_factory=dict)
    status: str = "pending"
    published_at: Optional[str] = None


def place_order(session: Session, *, email: str, total_cents: int) -> OrderRow:
    with session.begin():
        order = session.insert(
            OrderRow(
                customer_email=email,
                total_cents=total_cents,
                status="created",
            )
        )
        session.insert(
            OutboxMessage(
                aggregate_type="order",
                aggregate_id=str(order.id),
                event_type="order.created",
                payload={
                    "order_id": order.id,
                    "customer_email": email,
                    "total_cents": total_cents,
                },
            )
        )
    return order


def place_order_with_failure(session: Session) -> None:
    try:
        with session.begin():
            order = session.insert(
                OrderRow(
                    customer_email="rollback@example.com",
                    total_cents=9999,
                )
            )
            session.insert(
                OutboxMessage(
                    aggregate_type="order",
                    aggregate_id=str(order.id),
                    event_type="order.created",
                    payload={"order_id": order.id},
                )
            )
            raise RuntimeError("simulated failure before commit")
    except RuntimeError as exc:
        print("Expected rollback:", exc)


def publish_pending_messages(session: Session) -> None:
    pending = session.list(
        OutboxMessage,
        where=C.eq("status", "pending"),
        order_by=[OrderBy("id")],
    )
    for message in pending:
        with session.begin():
            updated = session.update_where(
                OutboxMessage,
                {
                    "status": "published",
                    "published_at": datetime.now(timezone.utc).isoformat(),
                },
                where=C.and_(
                    C.eq("id", message.id),
                    C.eq("status", "pending"),
                ),
            )
            if updated == 0:
                continue
            print("Publishing:", message.event_type, message.payload)


def main() -> None:
    conn = sqlite3.connect(":memory:")
    try:
        db = Database(conn, SQLiteDialect())
        session = Session(db, auto_schema=True)

        order_1 = place_order(session, email="alice@example.com", total_cents=1599)
        order_2 = place_order(session, email="bob@example.com", total_cents=2499)
        place_order_with_failure(session)

        print("Orders persisted:", session.count(OrderRow))
        print(
            "Pending outbox before publish:",
            session.count(OutboxMessage, where=C.eq("status", "pending")),
        )
        print("Created orders:", order_1, order_2)

        publish_pending_messages(session)

        print(
            "Pending outbox after publish:",
            session.count(OutboxMessage, where=C.eq("status", "pending")),
        )
        print(
            "Published outbox messages:",
            session.count(OutboxMessage, where=C.eq("status", "published")),
        )
    finally:
        conn.close()


if __name__ == "__main__":
    main()
