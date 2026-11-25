# Databricks VS Code Extension — Install & Configure

How to set up the Databricks extension for VS Code (or Kiro) against this repo, matching the working configuration already in place in `gr.cepal.dna.de.delivery`.

This lets you run notebooks/scripts locally against a Databricks all-purpose cluster via Databricks Connect, sync files to your workspace, and deploy Asset Bundles.

---

## 1. Prerequisites

- **VS Code / Kiro** (the extension works in both).
- **Python 3.11** — the repo pins `requires-python = "==3.11.*"` in `pyproject.toml`. Databricks Connect 15.4 requires 3.11 specifically.
- **A Databricks account** with access to the dev workspace: `https://dbc-1c3bb27b-2d8f.cloud.databricks.com`.
- Access to an **all-purpose (interactive) cluster** on that workspace. The current dev cluster is `0506-121850-11rwc2ls`.
- **`uv`** (recommended) for managing the virtual environment. If you don't have it: https://docs.astral.sh/uv/getting-started/installation/

---

## 2. Install the Extension

The repo already recommends the extension. `.vscode/extensions.json` contains:

```json
{
    "recommendations": [
        "databricks.databricks"
    ]
}
```

- Open the repo in VS Code/Kiro. You should get a prompt to install recommended extensions — accept it.
- Or install manually: Extensions view → search **"Databricks"** → install the one published by **Databricks** (id `databricks.databricks`).

---

## 3. Create the Python Environment

From the repo root:

```bash
uv venv --python 3.11 .venv
uv sync
```

This creates `.venv/` and installs `databricks-connect~=15.4.0` (from the `dev` dependency group in `pyproject.toml`) plus the pinned constraint set.

The workspace already points VS Code at this interpreter via `.vscode/settings.json`:

```json
{
  "python.defaultInterpreterPath": "${workspaceFolder}/.venv/bin/python",
  "databricks.python.envFile": "${workspaceFolder}/.databricks/.databricks.env"
}
```

If the interpreter isn't picked up automatically: Command Palette → **Python: Select Interpreter** → choose `./.venv/bin/python`.

---

## 4. Configure the Databricks Bundle

The repo ships a `databricks.yml` Asset Bundle at the root:

```yaml
bundle:
  name: gr-cepal-dna-de-delivery

targets:
  dev:
    mode: development
    default: true
    workspace:
      host: https://dbc-1c3bb27b-2d8f.cloud.databricks.com
    resources:
      clusters: {}
```

The extension reads this automatically. The `dev` target is the default, so you don't need to select it manually.

---

## 5. Sign In & Connect

1. Click the **Databricks** icon in the Activity Bar (side panel).
2. **Configure the workspace host** — it should auto-fill from `databricks.yml` (`https://dbc-1c3bb27b-2d8f.cloud.databricks.com`). If prompted, confirm it.
3. **Authenticate.** Pick an auth method:
   - **OAuth (U2M)** — recommended. A browser opens, you log in, done. No token to manage.
   - **Personal Access Token (PAT)** — generate one in the Databricks UI (User Settings → Developer → Access tokens) and paste it when prompted.
4. **Select the cluster** — under the "Cluster" section in the Databricks panel, attach to the dev all-purpose cluster `0506-121850-11rwc2ls`. This enables Databricks Connect for interactive runs.

Once connected, the panel shows your workspace host, the bundle target (`dev`), and the attached cluster.

---

## 6. The Generated `.databricks/` Folder

After you sign in and attach a cluster, the extension generates `.databricks/` — **this is machine-local and must never be committed**. It contains:

- `.databricks.env` — environment variables the extension injects into your Python runs. Example shape (values are per-machine and include a **live token**, so keep it local):

  ```
  DATABRICKS_AUTH_TYPE=metadata-service
  DATABRICKS_BUNDLE_TARGET=dev
  DATABRICKS_CLUSTER_ID=<your-cluster-id>
  DATABRICKS_HOST=https://dbc-1c3bb27b-2d8f.cloud.databricks.com/
  DATABRICKS_METADATA_SERVICE_URL=http://127.0.0.1:<port>/<uuid>
  DATABRICKS_PROJECT_ROOT=<abs-path-to-repo>
  SPARK_CONNECT_USER_AGENT=databricks-vscode/<version>
  SPARK_REMOTE=sc://...;token=<REDACTED>;...
  ```

- `bundle/dev/` — bundle state/artifacts for the dev target.

> **Security:** `.databricks/.databricks.env` and the `SPARK_REMOTE` line embed a Databricks token. Do not paste this file into chats, tickets, or commits. Rotate the token if it ever leaks.

### `.gitignore` coverage (already in place)

The repo root `.gitignore` ignores the whole folder:

```
# Databricks
.databricks/
```

and `.databricks/.gitignore` ignores everything inside it (`*`). So the folder is safe by default — just don't force-add it.

---

## 7. Verify the Setup

- **Run a notebook/file interactively:** open any notebook (e.g. under `RCG/`) and use the extension's **Run** on Databricks, or run a Python file with the Databricks Connect interpreter. It should execute against the attached cluster.
- **Quick Spark check** in a Python file:

  ```python
  from pyspark.sql import SparkSession
  spark = SparkSession.builder.getOrCreate()
  print(spark.range(5).count())  # -> 5
  ```

- **Bundle validation** from the terminal:

  ```bash
  databricks bundle validate -t dev
  ```

If these succeed, you're connected and ready to work.

---

## 8. Troubleshooting

- **Wrong Python version:** Databricks Connect 15.4 needs Python 3.11. `uv venv --python 3.11 .venv` and re-select the interpreter.
- **Interpreter not detected:** Command Palette → **Python: Select Interpreter** → `./.venv/bin/python`.
- **`.databricks.env` not applied:** confirm `databricks.python.envFile` in `.vscode/settings.json` points to `${workspaceFolder}/.databricks/.databricks.env`, then reload the window.
- **Auth expired / cluster detached:** re-open the Databricks panel, re-authenticate, and re-attach the cluster. This regenerates `.databricks.env`.
- **Cluster not listed:** make sure you have permission on cluster `0506-121850-11rwc2ls` (or ask an admin), or pick another Unity Catalog-enabled all-purpose cluster.
