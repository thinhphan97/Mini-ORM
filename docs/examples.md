# Examples

This page provides an interactive examples browser:
- the right panel is a sticky examples list
- clicking an item updates the content panel on the left
- each item shows the full source code of that example

## Run examples

Run from the project root.

```bash
make examples
make examples-sql
make examples-vector
```

Run one example:

```bash
make example-sql FILE=examples/sql/01_basic_crud.py
make example-vector FILE=examples/vector/01_inmemory_basic.py
```

## External services (optional)

Some examples require Postgres/PgVector, MySQL, Qdrant, or Chroma.

```bash
make compose-up
make compose-ps
```

Connection settings are read from environment variables (see project `README.md`).

## Interactive Browser

<style>
  .examples-browser {
    display: block;
  }

  .examples-panel {
    border: 1px solid var(--md-default-fg-color--lightest);
    border-radius: 12px;
    padding: 16px;
    background: var(--md-default-bg-color);
  }

  .examples-panel h3 {
    margin-top: 0;
  }

  .examples-meta {
    margin: 8px 0 12px;
    color: var(--md-default-fg-color--light);
    font-size: 0.85rem;
  }

  .examples-run {
    margin: 12px 0;
  }

  .examples-source {
    margin-top: 12px;
    border-top: 1px solid var(--md-default-fg-color--lightest);
    padding-top: 12px;
  }

  .examples-source-meta {
    margin: 6px 0;
    color: var(--md-default-fg-color--light);
    font-size: 0.85rem;
    word-break: break-word;
  }

  .examples-source pre {
    max-height: 640px;
    overflow: auto;
  }

  .examples-source code {
    display: block;
    color: var(--md-code-fg-color);
  }

  .examples-source code .tok-keyword {
    color: #c678dd;
    font-weight: 600;
  }

  .examples-source code .tok-builtin {
    color: #56b6c2;
    font-weight: 600;
  }

  .examples-source code .tok-string {
    color: #98c379;
  }

  .examples-source code .tok-comment {
    color: #7f8c8d;
    font-style: italic;
  }

  .examples-source code .tok-number {
    color: #d19a66;
  }

  .examples-source code .tok-decorator {
    color: #e5c07b;
  }

  .examples-sidebar {
    position: static;
    margin-top: 14px;
    max-height: 420px;
    overflow: auto;
    border: 1px solid var(--md-default-fg-color--lightest);
    border-radius: 12px;
    padding: 12px;
    background: var(--md-default-bg-color);
  }

  .examples-sidebar.docked {
    margin-top: 12px;
    max-height: none;
  }

  .examples-sidebar h4 {
    margin: 4px 0 10px;
  }

  .examples-group {
    margin: 0 0 10px;
  }

  .examples-group-title {
    font-size: 0.78rem;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    color: var(--md-default-fg-color--light);
    margin: 10px 0 6px;
  }

  .examples-nav-item {
    display: block;
    width: 100%;
    text-align: left;
    border: 1px solid transparent;
    border-radius: 8px;
    padding: 8px 10px;
    margin: 4px 0;
    cursor: pointer;
    background: transparent;
    color: var(--md-default-fg-color);
    font: inherit;
    line-height: 1.25;
  }

  .examples-nav-item:hover {
    border-color: var(--md-default-fg-color--lightest);
    background: var(--md-code-bg-color);
  }

  .examples-nav-item.active {
    border-color: var(--md-accent-fg-color);
    background: color-mix(in srgb, var(--md-accent-fg-color) 8%, transparent);
  }

  @media (max-width: 1219px) {
    .examples-sidebar {
      max-height: none;
    }
  }
</style>

<div class="examples-browser">
  <div class="examples-panel" id="examples-panel">
    <h3 id="example-title">Loading example...</h3>
    <div class="examples-meta">
      <code id="example-path"></code>
    </div>
    <ul id="example-summary"></ul>
    <div class="examples-run">
      <div>Run command:</div>
      <pre><code id="example-command"></code></pre>
    </div>
    <div class="examples-source" id="example-source-wrap">
      <h4>Attached code example</h4>
      <div class="examples-source-meta" id="example-source-path"></div>
      <pre><code id="example-source-code"></code></pre>
    </div>
  </div>
  <aside class="examples-sidebar" id="examples-sidebar">
    <h4>Examples</h4>
    <div id="examples-nav"></div>
  </aside>
</div>

<script src="../examples_sources.js"></script>
<script>
(() => {
  const groups = [
    {
      name: "SQL",
      items: [
        {
          id: "sql-01",
          title: "01 Basic CRUD",
          path: "examples/sql/01_basic_crud.py",
          command: "python examples/sql/01_basic_crud.py",
          summary: [
            "Dataclass model with auto primary key",
            "Schema apply from metadata",
            "Repository CRUD: insert/get/update/delete/list",
          ],
        },
        {
          id: "sql-02",
          title: "02 Query Conditions",
          path: "examples/sql/02_query_conditions.py",
          command: "python examples/sql/02_query_conditions.py",
          summary: [
            "Condition factory coverage: eq/ne/lt/le/gt/ge/like/is_null/is_not_null/in_",
            "Grouped expressions: and_/or_/not_",
            "Sorting, pagination, count, and exists",
          ],
        },
        {
          id: "sql-03",
          title: "03 Repository Utilities",
          path: "examples/sql/03_repository_utilities.py",
          command: "python examples/sql/03_repository_utilities.py",
          summary: [
            "Batch insert with insert_many",
            "update_where and delete_where",
            "get_or_create flow",
          ],
        },
        {
          id: "sql-04",
          title: "04 Schema and Indexes",
          path: "examples/sql/04_schema_and_indexes.py",
          command: "python examples/sql/04_schema_and_indexes.py",
          summary: [
            "SQL generation helpers for table/schema/index",
            "Field-level indexes and __indexes__",
            "Idempotent apply_schema(..., if_not_exists=True)",
          ],
        },
        {
          id: "sql-05",
          title: "05 Validation and Errors",
          path: "examples/sql/05_validation_and_error_cases.py",
          command: "python examples/sql/05_validation_and_error_cases.py",
          summary: [
            "Common error scenarios",
            "Validation and expected exceptions",
          ],
        },
        {
          id: "sql-06",
          title: "06 Dialect Preview",
          path: "examples/sql/06_dialect_preview.py",
          command: "python examples/sql/06_dialect_preview.py",
          summary: [
            "Compares SQLite/Postgres/MySQL SQL generation",
            "Dialect-level differences in output",
          ],
        },
        {
          id: "sql-07",
          title: "07 Relations Create/Query",
          path: "examples/sql/07_relations_create_and_query.py",
          command: "python examples/sql/07_relations_create_and_query.py",
          summary: [
            "Infer relations from fk metadata",
            "Nested create via relations=...",
            "get_related and list_related",
          ],
        },
        {
          id: "sql-08",
          title: "08 Codec Serialize/Deserialize",
          path: "examples/sql/08_codec_serialize_deserialize.py",
          command: "python examples/sql/08_codec_serialize_deserialize.py",
          summary: [
            "Enum and JSON codec IO flow",
            "Raw stored values vs decoded model values",
            "insert/get/list/update_where coverage",
          ],
        },
        {
          id: "sql-09",
          title: "09 Async Basic CRUD",
          path: "examples/sql/09_async_basic_crud.py",
          command: "python examples/sql/09_async_basic_crud.py",
          summary: [
            "Async API with same method names as sync",
            "AsyncDatabase + AsyncRepository(auto_schema=True)",
          ],
        },
        {
          id: "sql-10",
          title: "10 Async Postgres",
          path: "examples/sql/10_async_postgres_example.py",
          command: "python examples/sql/10_async_postgres_example.py",
          summary: [
            "Async SQL with PostgresDialect",
            "Optional dependency/service script with safe skip",
          ],
        },
        {
          id: "sql-11",
          title: "11 Async MySQL",
          path: "examples/sql/11_async_mysql_example.py",
          command: "python examples/sql/11_async_mysql_example.py",
          summary: [
            "Async SQL with MySQLDialect",
            "Optional dependency/service script with safe skip",
          ],
        },
        {
          id: "sql-12",
          title: "12 Unified Repository",
          path: "examples/sql/12_unified_repository.py",
          command: "python examples/sql/12_unified_repository.py",
          summary: [
            "Single hub for multiple model classes",
            "auto_schema + require_registration with register_many",
          ],
        },
        {
          id: "sql-13",
          title: "13 Async Unified Repository",
          path: "examples/sql/13_async_unified_repository.py",
          command: "python examples/sql/13_async_unified_repository.py",
          summary: [
            "Async unified hub flow",
            "Mutation methods infer model from object",
            "Query methods still pass model class",
          ],
        },
        {
          id: "sql-14",
          title: "14 Validated Model",
          path: "examples/sql/14_validated_model.py",
          command: "python examples/sql/14_validated_model.py",
          summary: [
            "ValidatedModel for pydantic-like checks",
            "Field constraints via metadata",
            "ValidationError on invalid input",
          ],
        },
        {
          id: "sql-15",
          title: "15 Validated Repository SQLite",
          path: "examples/sql/15_validated_repository_sqlite.py",
          command: "python examples/sql/15_validated_repository_sqlite.py",
          summary: [
            "ValidatedModel + Repository on SQLite",
            "auto_schema first-use setup",
            "Friendly invalid-input handling",
          ],
        },
        {
          id: "sql-16",
          title: "16 Postgres + PgVector Integration",
          path: "examples/sql/16_postgres_pgvector_integration.py",
          command: "python examples/sql/16_postgres_pgvector_integration.py",
          summary: [
            "One Database for SQL Repository and PgVectorStore",
            "Map vector hits back to SQL rows",
            "Requires Postgres + pgvector and psycopg/psycopg2",
          ],
        },
        {
          id: "sql-17",
          title: "17 Session Usage",
          path: "examples/sql/17_session_usage.py",
          command: "python examples/sql/17_session_usage.py",
          summary: [
            "Session and AsyncSession wrappers",
            "Transaction commit and rollback behavior",
            "Session-level auto_schema flow",
          ],
        },
        {
          id: "sql-18",
          title: "18 Outbox Pattern",
          path: "examples/sql/18_outbox_pattern.py",
          command: "python examples/sql/18_outbox_pattern.py",
          summary: [
            "Transactional outbox with Session",
            "Business row and outbox message in one commit",
            "Rollback simulation and publisher step",
          ],
        },
      ],
    },
    {
      name: "Vector",
      items: [
        {
          id: "vec-01",
          title: "01 InMemory Basic",
          path: "examples/vector/01_inmemory_basic.py",
          command: "python examples/vector/01_inmemory_basic.py",
          summary: [
            "Basic VectorRepository flow with InMemoryVectorStore",
            "upsert/query/fetch/delete usage",
          ],
        },
        {
          id: "vec-02",
          title: "02 Metrics and Filters",
          path: "examples/vector/02_inmemory_metrics_and_filters.py",
          command: "python examples/vector/02_inmemory_metrics_and_filters.py",
          summary: [
            "Metric behavior: cosine/dot/l2",
            "Payload equality filters",
          ],
        },
        {
          id: "vec-03",
          title: "03 Lifecycle and Errors",
          path: "examples/vector/03_repository_lifecycle_and_errors.py",
          command: "python examples/vector/03_repository_lifecycle_and_errors.py",
          summary: [
            "auto_create=False with manual create_collection",
            "overwrite and dimension checks",
            "filter support and UUID policy behavior",
          ],
        },
        {
          id: "vec-04",
          title: "04 Qdrant",
          path: "examples/vector/04_qdrant_example.py",
          command: "python examples/vector/04_qdrant_example.py",
          summary: [
            "Qdrant usage and UUID ID policy",
            "Optional dependency script",
          ],
        },
        {
          id: "vec-05",
          title: "05 Chroma",
          path: "examples/vector/05_chroma_example.py",
          command: "python examples/vector/05_chroma_example.py",
          summary: [
            "Chroma usage with in-memory collection",
            "Filter examples and optional dependency script",
          ],
        },
        {
          id: "vec-06",
          title: "06 Faiss",
          path: "examples/vector/06_faiss_example.py",
          command: "python examples/vector/06_faiss_example.py",
          summary: [
            "Faiss usage and unsupported-filter behavior",
            "Optional dependency script",
          ],
        },
        {
          id: "vec-07",
          title: "07 Payload Codec",
          path: "examples/vector/07_payload_codec.py",
          command: "python examples/vector/07_payload_codec.py",
          summary: [
            "JsonVectorPayloadCodec usage",
            "Raw payload vs decoded payload",
            "Filter queries with Enum values",
          ],
        },
        {
          id: "vec-08",
          title: "08 Async InMemory Basic",
          path: "examples/vector/08_async_inmemory_basic.py",
          command: "python examples/vector/08_async_inmemory_basic.py",
          summary: [
            "Async vector API with same method names as sync",
            "AsyncVectorRepository over InMemoryVectorStore",
          ],
        },
        {
          id: "vec-09",
          title: "09 Async Qdrant",
          path: "examples/vector/09_async_qdrant_example.py",
          command: "python examples/vector/09_async_qdrant_example.py",
          summary: [
            "Async Qdrant usage with UUID ID policy",
            "Optional dependency script",
          ],
        },
        {
          id: "vec-10",
          title: "10 Async Chroma",
          path: "examples/vector/10_async_chroma_example.py",
          command: "python examples/vector/10_async_chroma_example.py",
          summary: [
            "Async Chroma usage with filters",
            "Optional dependency script",
          ],
        },
        {
          id: "vec-11",
          title: "11 Async Faiss",
          path: "examples/vector/11_async_faiss_example.py",
          command: "python examples/vector/11_async_faiss_example.py",
          summary: [
            "Async Faiss usage",
            "Unsupported-filter behavior and optional dependency script",
          ],
        },
        {
          id: "vec-12",
          title: "12 PgVector",
          path: "examples/vector/12_pgvector_example.py",
          command: "python examples/vector/12_pgvector_example.py",
          summary: [
            "PgVectorStore + VectorRepository flow",
            "Requires Postgres + pgvector and psycopg/psycopg2",
          ],
        },
      ],
    },
  ];

  const allItems = groups.flatMap((group) => group.items);
  const panelTitle = document.getElementById("example-title");
  const panelPath = document.getElementById("example-path");
  const panelSummary = document.getElementById("example-summary");
  const panelCommand = document.getElementById("example-command");
  const panelSourcePath = document.getElementById("example-source-path");
  const panelSourceCode = document.getElementById("example-source-code");
  const navRoot = document.getElementById("examples-nav");
  const browserRoot = document.querySelector(".examples-browser");
  const sidebar = document.getElementById("examples-sidebar");
  const sourceStore = window.MINI_ORM_EXAMPLE_SOURCES || {};
  const PYTHON_KEYWORDS = new Set([
    "and", "as", "assert", "async", "await", "break", "class", "continue",
    "def", "del", "elif", "else", "except", "finally", "for", "from", "global",
    "if", "import", "in", "is", "lambda", "nonlocal", "not", "or", "pass",
    "raise", "return", "try", "while", "with", "yield", "match", "case",
  ]);
  const PYTHON_BUILTINS = new Set(["True", "False", "None"]);

  function dockExamplesLayer() {
    const secondarySidebar = document.querySelector(".md-sidebar--secondary .md-sidebar__scrollwrap");
    const canDock = window.matchMedia("(min-width: 1220px)").matches && secondarySidebar;

    if (canDock) {
      if (!sidebar.classList.contains("docked")) {
        secondarySidebar.appendChild(sidebar);
        sidebar.classList.add("docked");
      }
      return;
    }

    if (sidebar.classList.contains("docked")) {
      browserRoot.appendChild(sidebar);
      sidebar.classList.remove("docked");
    }
  }

  function escapeHtml(text) {
    return text
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;");
  }

  function isIdentifierStart(ch) {
    return /[A-Za-z_]/.test(ch);
  }

  function isIdentifierPart(ch) {
    return /[A-Za-z0-9_]/.test(ch);
  }

  function isDigit(ch) {
    return /[0-9]/.test(ch);
  }

  function readStringToken(source, start, quote) {
    const triple = source.slice(start, start + 3) === quote.repeat(3);
    let i = start + (triple ? 3 : 1);

    while (i < source.length) {
      if (triple && source.slice(i, i + 3) === quote.repeat(3)) {
        i += 3;
        break;
      }
      if (!triple && source[i] === quote) {
        i += 1;
        break;
      }
      if (source[i] === "\\") {
        i += 2;
        continue;
      }
      i += 1;
    }

    return { value: source.slice(start, i), end: i };
  }

  function readIdentifier(source, start) {
    let i = start;
    while (i < source.length && isIdentifierPart(source[i])) {
      i += 1;
    }
    return { value: source.slice(start, i), end: i };
  }

  function readNumber(source, start) {
    let i = start;
    while (i < source.length && /[0-9A-Fa-f_xob\.]/.test(source[i])) {
      i += 1;
    }
    return { value: source.slice(start, i), end: i };
  }

  function highlightPythonBasic(source) {
    let i = 0;
    let highlighted = "";

    while (i < source.length) {
      const ch = source[i];

      if (ch === "#") {
        let end = i;
        while (end < source.length && source[end] !== "\n") {
          end += 1;
        }
        highlighted += `<span class="tok-comment">${escapeHtml(source.slice(i, end))}</span>`;
        i = end;
        continue;
      }

      if (ch === "'" || ch === "\"") {
        const token = readStringToken(source, i, ch);
        highlighted += `<span class="tok-string">${escapeHtml(token.value)}</span>`;
        i = token.end;
        continue;
      }

      if (ch === "@" && (i === 0 || source[i - 1] === "\n")) {
        const token = readIdentifier(source, i + 1);
        highlighted += `<span class="tok-decorator">@${escapeHtml(token.value)}</span>`;
        i = token.end;
        continue;
      }

      if (isDigit(ch)) {
        const token = readNumber(source, i);
        highlighted += `<span class="tok-number">${escapeHtml(token.value)}</span>`;
        i = token.end;
        continue;
      }

      if (isIdentifierStart(ch)) {
        const token = readIdentifier(source, i);
        if (PYTHON_KEYWORDS.has(token.value)) {
          highlighted += `<span class="tok-keyword">${token.value}</span>`;
        } else if (PYTHON_BUILTINS.has(token.value)) {
          highlighted += `<span class="tok-builtin">${token.value}</span>`;
        } else {
          highlighted += escapeHtml(token.value);
        }
        i = token.end;
        continue;
      }

      highlighted += escapeHtml(ch);
      i += 1;
    }

    return highlighted;
  }

  function renderSourceCode(path) {
    panelSourcePath.textContent = path;
    const source = sourceStore[path];
    if (typeof source !== "string") {
      panelSourceCode.textContent = `Source not available for ${path}`;
      return;
    }

    const hljs = window.hljs;
    if (hljs && typeof hljs.highlight === "function") {
      try {
        const result = hljs.highlight(source, {
          language: "python",
          ignoreIllegals: true,
        });
        panelSourceCode.innerHTML = result.value;
        return;
      } catch (_err) {
        // Fallback to basic highlighter below.
      }
    }

    panelSourceCode.innerHTML = highlightPythonBasic(source);
  }

  function renderExample(exampleId) {
    const current = allItems.find((item) => item.id === exampleId) || allItems[0];
    panelTitle.textContent = current.title;
    panelPath.textContent = current.path;
    panelCommand.textContent = current.command;
    renderSourceCode(current.path);

    panelSummary.innerHTML = "";
    current.summary.forEach((line) => {
      const item = document.createElement("li");
      item.textContent = line;
      panelSummary.appendChild(item);
    });
  }

  function renderNav(defaultId) {
    navRoot.innerHTML = "";
    groups.forEach((group) => {
      const wrapper = document.createElement("div");
      wrapper.className = "examples-group";

      const label = document.createElement("div");
      label.className = "examples-group-title";
      label.textContent = group.name;
      wrapper.appendChild(label);

      group.items.forEach((item) => {
        const button = document.createElement("button");
        button.type = "button";
        button.className = "examples-nav-item";
        button.dataset.id = item.id;
        button.textContent = item.title;
        if (item.id === defaultId) {
          button.classList.add("active");
        }
        button.addEventListener("click", () => {
          renderExample(item.id);
          document.querySelectorAll(".examples-nav-item").forEach((node) => {
            node.classList.remove("active");
          });
          button.classList.add("active");
        });
        wrapper.appendChild(button);
      });

      navRoot.appendChild(wrapper);
    });
  }

  const defaultId = allItems[0].id;
  renderNav(defaultId);
  renderExample(defaultId);
  dockExamplesLayer();
  window.addEventListener("resize", dockExamplesLayer);
})();
</script>

## Optional dependencies

```bash
pip install psycopg
pip install qdrant-client
pip install chromadb
pip install faiss-cpu numpy
```
