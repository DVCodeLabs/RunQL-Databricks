# RunQL Databricks Connector

[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](./LICENSE)
[![VS Code](https://img.shields.io/badge/vscode-%5E1.96.0-007ACC)](https://code.visualstudio.com/)

Optional Databricks connector for the [RunQL](https://marketplace.visualstudio.com/items?itemName=RunQL-VSCode-Extension.runql) VS Code extension. Install alongside RunQL to query Databricks SQL warehouses with RunQL's SQL workflows, results panel, schema introspection, and ERD tooling.

This extension is maintained separately so users who do not need Databricks are not required to install its SQL driver dependencies.

## Installation

1. Install [RunQL](https://marketplace.visualstudio.com/items?itemName=RunQL-VSCode-Extension.runql) (required, declared as an extension dependency).
2. Install **RunQL Databricks Connector** from the VS Code Marketplace.

RunQL will detect the connector on activation and register Databricks as an available connection provider.

## Usage

1. Open the RunQL explorer view.
2. Click **Add Connection** and choose **Databricks**.
3. Enter your Databricks workspace host.
4. Enter the SQL warehouse HTTP path.
5. Optionally enter a Unity Catalog catalog and schema.
6. On the **Auth** tab, enter a personal access token.
7. Save and test the connection.

## Requirements

- VS Code `^1.96.0`
- [RunQL](https://marketplace.visualstudio.com/items?itemName=RunQL-VSCode-Extension.runql) extension
- A Databricks workspace with a SQL warehouse or cluster SQL endpoint
- A Databricks personal access token with access to the target warehouse and metadata

## Authentication

The connector currently supports Databricks personal access token authentication.

## How it works

On activation, this extension acquires the RunQL extension API and calls:

- `registerProvider(databricksProvider)` to add Databricks to the connection form.
- `registerAdapter('databricks', () => new DatabricksAdapter())` to wire the dialect to its implementation.

The adapter uses the [`@databricks/sql`](https://www.npmjs.com/package/@databricks/sql) Node package. Schema introspection uses Databricks SQL metadata operations for catalogs, schemas, tables, and columns, plus best-effort Unity Catalog `information_schema` queries for keys, foreign keys, routines, and parameters where the workspace exposes that metadata.

## Building from source

```bash
git clone https://github.com/DVCodeLabs/RunQL-Databricks.git
cd RunQL-Databricks
npm install
npm run package
```

To produce a local VSIX for testing:

```bash
npm install -g @vscode/vsce
vsce package
```

## License

MIT, see [LICENSE](./LICENSE).
