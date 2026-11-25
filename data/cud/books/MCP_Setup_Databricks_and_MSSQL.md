# Setting up MCP Servers in Kiro — Databricks (Prod) & MSSQL

A hands-on guide for configuring two Model Context Protocol (MCP) servers in Kiro:

1. **Databricks (production)** — lets Kiro explore Unity Catalog and run SQL against a prod SQL Warehouse.
2. **MSSQL (ITStaging)** — read-only access to the on-prem `dwhcore/ITStaging` SQL Server database.

Once configured, you can ask Kiro things like *"list the schemas in the primary catalog"* or *"query the top 10 rows from dtransfer.Customer"* and it will call these tools directly.

> **Audience:** Any Kiro user on the team. No prior MCP experience needed.

---

## 1. What is MCP (30-second version)

MCP is a protocol that lets Kiro talk to external tools ("MCP servers"). Each server exposes a set of tools (e.g. `execute_sql_query`, `list_tables`) that Kiro can call on your behalf. You configure servers in a JSON file; Kiro launches them and routes tool calls to them.

Kiro reads MCP config from two places (they are merged):

| Level | Path | Scope |
|---|---|---|
| **User** | `~/.kiro/settings/mcp.json` | All workspaces on your machine |
| **Workspace** | `<repo>/.kiro/settings/mcp.json` | Only that workspace/repo |

If the same server name exists in both, the workspace-level entry wins.

---

## 2. Prerequisites

Install these once:

- **Python 3.10+**
- **`uv`** (the runner used to launch both servers) — install guide: <https://docs.astral.sh/uv/getting-started/installation/>
  - Verify with: `uv --version`
- **For MSSQL only:** the Microsoft ODBC Driver 18 for SQL Server, plus network access to the on-prem SQL Server host.

You also need the two MCP server codebases available locally:

- Databricks server: `/home/<you>/cepal/mcp-databricks-server`
- MSSQL server: `/home/<you>/.kiro/mcp-servers/mcp_mssql_server`

> Replace `<you>` with your username throughout. Paths in `mcp.json` must be **absolute** — Kiro does not expand `~`.

---

## 3. Security first — keep secrets OUT of `mcp.json`

MCP config files are a classic place for secrets to leak (tokens, client secrets, DB passwords). The rule for this setup is simple: **no secret value is ever written into `mcp.json`.** Instead, `mcp.json` references environment variables with `${VAR}` syntax, and the real values live only in your shell environment.

### 3.1 How Kiro expands `${VAR}`

Kiro expands `${VARIABLE_NAME}` references in `mcp.json` using your environment — but **only for variables that are explicitly approved**. Any variable not on the approval list is passed through literally as the string `${VAR}`, which shows up as an auth failure. This is a deliberate guardrail so an MCP server can't read arbitrary environment variables.

For the Kiro CLI, the approval list lives in `~/.kiro/settings/cli.json`:

```json
{
  "kiroAgent.mcpApprovedEnvVars": [
    "DATABRICKS_TOKEN",
    "DATABRICKS_CLIENT_ID",
    "DATABRICKS_CLIENT_SECRET"
  ]
}
```

In the Kiro IDE, the same approval mechanism applies; see the official MCP best-practices page for the current UI/setting: <https://kiro.dev/docs/mcp/security/>.

### 3.2 Set the secrets in your shell, not the config

Export the sensitive values from your shell profile (`~/.bashrc` / `~/.zshrc`), so they never touch any file in the repo:

```bash
export DATABRICKS_CLIENT_SECRET="your-service-principal-secret"
# or, if using a PAT:
export DATABRICKS_TOKEN="your-personal-access-token"
```

Then reference them in `mcp.json` as `${DATABRICKS_CLIENT_SECRET}` / `${DATABRICKS_TOKEN}` (see the examples below).

### 3.3 Defense in depth

Even with no secrets in the file, keep these habits:
- **Still git-ignore `mcp.json`.** Confirm the rule exists in your repo's `.gitignore`:
  ```
  .kiro/settings/mcp.json
  **/mcp.json
  ```
- If you ever must hardcode a value temporarily, `chmod 600 .kiro/settings/mcp.json` and never commit it.
- Never paste real tokens into shared docs, chat, or screenshots.
- Prefer a **service principal** with least-privilege for Databricks prod, not your personal token.
- Rotate a secret by updating the env var only — no need to touch any `mcp.json`.
- The MSSQL account credentials are not in `mcp.json` either; they live inside the server code (`server.py`).

---

## 4. MCP Server #1 — Databricks (Production)

### 4.1 One-time server install

```bash
cd /home/<you>/cepal/mcp-databricks-server
uv pip install -r requirements.txt
```

### 4.2 Gather your Databricks connection values

You need three values from your prod workspace:

| Value | Where to find it |
|---|---|
| `DATABRICKS_HOST` | Your workspace URL, e.g. `dbc-xxxxxxxx-xxxx.cloud.databricks.com` (no `https://`) |
| Auth | Either a **personal access token** (`DATABRICKS_TOKEN`) or a **service principal** (`DATABRICKS_CLIENT_ID` + `DATABRICKS_CLIENT_SECRET`) |
| `DATABRICKS_SQL_WAREHOUSE_ID` | Databricks UI → **SQL Warehouses** → your warehouse → copy its ID |

**Required permissions** for the identity behind the token/service principal:
- Unity Catalog: `USE CATALOG`, `USE SCHEMA`, `SELECT` on the assets you want to browse/query.
- SQL Warehouse: `CAN_USE` on the warehouse referenced by `DATABRICKS_SQL_WAREHOUSE_ID`.

### 4.3 Add the server to `mcp.json`

Open (or create) `<repo>/.kiro/settings/mcp.json` and add a `databricks-prod` entry. Secret values are **referenced** via `${VAR}`, not written in. `DATABRICKS_HOST` and `DATABRICKS_SQL_WAREHOUSE_ID` are not secrets, so they can stay inline. Two variants depending on your auth method:

**Service principal (recommended for prod):**

```json
{
  "mcpServers": {
    "databricks-prod": {
      "command": "/home/<you>/.local/bin/uv",
      "args": [
        "--directory",
        "/home/<you>/cepal/mcp-databricks-server",
        "run",
        "main.py"
      ],
      "env": {
        "DATABRICKS_HOST": "dbc-xxxxxxxx-xxxx.cloud.databricks.com",
        "DATABRICKS_CLIENT_ID": "${DATABRICKS_CLIENT_ID}",
        "DATABRICKS_CLIENT_SECRET": "${DATABRICKS_CLIENT_SECRET}",
        "DATABRICKS_SQL_WAREHOUSE_ID": "<your-warehouse-id>"
      },
      "disabled": false,
      "autoApprove": [
        "describe_uc_table"
      ]
    }
  }
}
```

**Personal access token (simpler, good for a quick demo):**

```json
{
  "mcpServers": {
    "databricks-prod": {
      "command": "/home/<you>/.local/bin/uv",
      "args": [
        "--directory",
        "/home/<you>/cepal/mcp-databricks-server",
        "run",
        "main.py"
      ],
      "env": {
        "DATABRICKS_HOST": "dbc-xxxxxxxx-xxxx.cloud.databricks.com",
        "DATABRICKS_TOKEN": "${DATABRICKS_TOKEN}",
        "DATABRICKS_SQL_WAREHOUSE_ID": "<your-warehouse-id>"
      },
      "disabled": false,
      "autoApprove": []
    }
  }
}
```

Field notes:
- **`command`** must be the absolute path to `uv` (find yours with `which uv`).
- **`${DATABRICKS_TOKEN}` / `${DATABRICKS_CLIENT_SECRET}`** are expanded from your shell environment — see section 3. Export the value in your shell profile and add the variable name to the approved-env-vars list, or Kiro will pass the literal `${...}` string and auth will fail.
- **`autoApprove`** lists tools Kiro can run without asking each time. Keep this short for prod — read-only describe tools are a safe start; leave `execute_sql_query` off the list so you approve each query.
- **`disabled`** set to `false` to enable the server.

### 4.4 Available Databricks tools

Once connected, Kiro can call:

| Tool | What it does |
|---|---|
| `list_uc_catalogs()` | List all Unity Catalogs |
| `describe_uc_catalog(catalog_name)` | List schemas in a catalog |
| `describe_uc_schema(catalog_name, schema_name, include_columns?)` | List tables (optionally columns) in a schema |
| `describe_uc_table(full_table_name, include_lineage?)` | Describe a table; optional lineage (upstream/downstream tables, notebooks, jobs) |
| `execute_sql_query(sql)` | Run a SQL query against the warehouse |

---

## 5. MCP Server #2 — MSSQL (ITStaging)

This server gives Kiro **read-only** access to the on-prem `dwhcore/ITStaging` database. Its connection details (server host, database, credentials) are baked into the server code (`server.py`), so `mcp.json` only needs to point Kiro at the server — no secrets in the JSON.

### 5.1 One-time server install

```bash
cd /home/<you>/.kiro/mcp-servers/mcp_mssql_server
uv sync
```

Dependencies (from `pyproject.toml`): `mcp==1.2.0`, `pyodbc>=5.0.0`. `pyodbc` also requires the system **ODBC Driver 18 for SQL Server** to be installed and network reachability to the SQL Server host.

### 5.2 Add the server to `mcp.json`

Add an `mssql-itstaging` entry alongside the Databricks one:

```json
{
  "mcpServers": {
    "mssql-itstaging": {
      "command": "/home/<you>/.local/bin/uv",
      "args": [
        "--directory",
        "/home/<you>/.kiro/mcp-servers/mcp_mssql_server",
        "run",
        "server.py"
      ],
      "disabled": false,
      "autoApprove": [
        "query",
        "list_tables",
        "list_columns"
      ]
    }
  }
}
```

Notes:
- No `env` block needed — the connection string lives in `server.py`.
- All three tools are read-only, so auto-approving them is reasonable for a demo. Adjust to taste.

### 5.3 Available MSSQL tools

| Tool | What it does |
|---|---|
| `query(sql)` | Run a read-only (SELECT) query; returns JSON |
| `list_tables(schema_name)` | List tables in a schema (e.g. `dtransfer`, `qualcoalpha`) |
| `list_columns(schema_name, table_name)` | List columns for a table |

---

## 6. Full example `mcp.json` (both servers)

Combine both into a single workspace file at `<repo>/.kiro/settings/mcp.json`. Note there are **no secret values** in this file — the Databricks secrets come in via `${...}` references (see section 3), and MSSQL has no `env` block at all:

```json
{
  "mcpServers": {
    "databricks-prod": {
      "command": "/home/<you>/.local/bin/uv",
      "args": [
        "--directory",
        "/home/<you>/cepal/mcp-databricks-server",
        "run",
        "main.py"
      ],
      "env": {
        "DATABRICKS_HOST": "dbc-xxxxxxxx-xxxx.cloud.databricks.com",
        "DATABRICKS_CLIENT_ID": "${DATABRICKS_CLIENT_ID}",
        "DATABRICKS_CLIENT_SECRET": "${DATABRICKS_CLIENT_SECRET}",
        "DATABRICKS_SQL_WAREHOUSE_ID": "<your-warehouse-id>"
      },
      "disabled": false,
      "autoApprove": ["describe_uc_table"]
    },
    "mssql-itstaging": {
      "command": "/home/<you>/.local/bin/uv",
      "args": [
        "--directory",
        "/home/<you>/.kiro/mcp-servers/mcp_mssql_server",
        "run",
        "server.py"
      ],
      "disabled": false,
      "autoApprove": ["query", "list_tables", "list_columns"]
    }
  }
}
```

---

## 7. Connect and verify

1. Save `mcp.json`. Kiro picks up config changes automatically — you can also reconnect from the **MCP Server** view in the Kiro feature panel without restarting.
2. Open the **MCP Server** panel and confirm both `databricks-prod` and `mssql-itstaging` show as connected (green).
3. Smoke-test each server by asking Kiro:
   - Databricks: *"List the Unity Catalogs"* → should call `list_uc_catalogs()`.
   - MSSQL: *"List the tables in the dtransfer schema"* → should call `list_tables("dtransfer")`.

---

## 8. Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| Server shows "failed to start" | Wrong absolute path in `command` or `--directory` | Run `which uv`; confirm the server directory exists |
| `command not found` | `uv` not installed or not on the path Kiro sees | Install `uv`; use its absolute path in `command` |
| Databricks auth errors (401 / unauthorized) | `${VAR}` not expanded — variable missing from the approved list or not exported in your shell | Add the name to `kiroAgent.mcpApprovedEnvVars` in `~/.kiro/settings/cli.json` (CLI) / the IDE approval setting, `export` it in your shell profile, then restart Kiro |
| Databricks auth errors | Bad token/secret, or missing UC/warehouse permissions | Re-check credentials; verify `USE CATALOG`/`SELECT`/`CAN_USE` grants |
| Databricks queries time out | Long-running SQL (server waits ~50s) | Narrow the query; add `LIMIT` |
| MSSQL connection error | ODBC driver missing or host unreachable | Install ODBC Driver 18; confirm network access to the SQL Server host |
| Server connects but tools not auto-running | Tool not in `autoApprove` | Add the tool name, or approve interactively when prompted |
| Secrets showing in `git status` | `mcp.json` not ignored | Ensure `.kiro/settings/mcp.json` is in `.gitignore` |

---

## 9. Demo talking points (quick script)

1. Show the empty MCP panel → paste the config → watch both servers go green.
2. Ask Kiro to list Databricks catalogs, then drill into a schema and describe a table.
3. Ask Kiro to run a small `SELECT` via `execute_sql_query` (approve it live to show the guardrail).
4. Switch to MSSQL: list tables in `dtransfer`, then list columns for one table.
5. Wrap up on security: secrets never live in `mcp.json` (referenced via `${VAR}` from the shell, gated by the approved-env-vars list), prod uses a least-privilege service principal, and read-only tools are safe to auto-approve.
