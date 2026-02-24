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

## Relations (create and query)

Declare relation intent on FK metadata and let mini_orm infer relation specs.

### 1) Minimal declaration

```python
@dataclass
class Author:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    name: str = ""

@dataclass
class Post:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    author_id: Optional[int] = field(
        default=None,
        metadata={"fk": (Author, "id")},
    )
    title: str = ""
```

From one FK field, mini_orm infers both sides:
- `Post.author` (`belongs_to`)
- `Author.posts` (`has_many`)

### 2) Optional relation naming

Use metadata keys to control relation names explicitly:

```python
@dataclass
class Author:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    name: str = ""

@dataclass
class Post:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    author_id: Optional[int] = field(
        default=None,
        metadata={
            "fk": (Author, "id"),
            "relation": "author",      # name for belongs_to on Post
            "related_name": "posts",   # reverse has_many name on Author
        },
    )
    title: str = ""
```

### 3) Default naming rules

If you omit explicit names:
- `relation` defaults from FK field name:
    `author_id` -> `author`
- `related_name` defaults from child model name pluralization:
    `Post` -> `posts`

For irregular names or multiple FKs to the same target model, set
`relation`/`related_name` explicitly to avoid ambiguity.

### 4) Create with nested relations

`Repository.create(..., relations=...)` supports both directions:

```python
# has_many path (create parent + children)
author_repo.create(
    Author(name="alice"),
    relations={"posts": [Post(title="p1"), Post(title="p2")]},
)

# belongs_to path (create child + nested parent)
post_repo.create(
    Post(title="hello"),
    relations={"author": Author(name="bob")},
)
```

### 5) Query with included relations

Use `include=[...]` with `get_related`/`list_related`:

```python
author_with_posts = author_repo.get_related(1, include=["posts"])
posts_with_author = post_repo.list_related(include=["author"])
```

### 6) Metadata formats supported for relation inference

Relation inference needs an FK that references a model class:
- `metadata={"fk": (Author, "id")}`
- `metadata={"fk": {"model": Author, "column": "id"}}`

`fk` string format (`"author.id"`) is valid for SQL schema generation, but it
does not carry the model class, so relation inference cannot build `include`
relations from it.

### 7) Explicit relation override

You can still define `__relations__` for full manual control.
Explicit declarations are kept and inferred duplicates are skipped.
This is also the recommended path when parent/child models are split across
different modules and you need guaranteed reverse `has_many` discovery.

### 8) Common validation errors

- `Unknown relation '...'` when calling `include`/`relations`:
  relation name is not inferred or declared.
- `expects a sequence` on has_many create:
  pass a list/tuple of model objects.
- `expects <ModelName>`:
  nested relation value type does not match the target model.
- duplicate inferred relation names:
  set `relation` and/or `related_name` to disambiguate.
