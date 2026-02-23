# Repository Queries

`Repository[T]` provides single-table CRUD with optional filtering and pagination.

## Insert / update / delete

```python
repo.insert(User(email="a@example.com", age=20))

user = repo.list(where=C.eq("email", "a@example.com"))[0]
user.age = 21
repo.update(user)

repo.delete(user)
```

## Filter and order

```python
from mini_orm import C, OrderBy

rows = repo.list(
    where=[C.ge("age", 18), C.lt("age", 30)],
    order_by=[OrderBy("age", desc=True), OrderBy("id", desc=False)],
    limit=20,
    offset=0,
)
```

`where` accepts one condition or a list of conditions joined by `AND`.

## Grouped conditions (`AND` / `OR` / `NOT`)

```python
rows = repo.list(
    where=C.and_(
        C.eq("active", True),
        C.or_(C.eq("role", "admin"), C.eq("role", "owner")),
        C.not_(C.eq("email", "blocked@example.com")),
    )
)
```

## Utility methods

```python
total = repo.count(where=C.like("email", "%@example.com"))
has_adult = repo.exists(where=C.ge("age", 18))

repo.insert_many([User(email="u1@example.com"), User(email="u2@example.com")])

repo.update_where({"age": 30}, where=C.eq("email", "u1@example.com"))
repo.delete_where(where=C.eq("email", "u2@example.com"))

row, created = repo.get_or_create(
    lookup={"email": "first@example.com"},
    defaults={"age": 20},
)
```

## Get by primary key

```python
row = repo.get(1)
```
