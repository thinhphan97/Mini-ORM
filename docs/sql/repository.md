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
    where=[C.gte("age", 18), C.lt("age", 30)],
    order_by=[OrderBy("age", "DESC"), OrderBy("id", "ASC")],
    limit=20,
    offset=0,
)
```

`where` accepts one condition or a list of conditions joined by `AND`.

## Get by primary key

```python
row = repo.get(1)
```
