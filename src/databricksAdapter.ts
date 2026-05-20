import { DBSQLClient } from '@databricks/sql';
import {
  ColumnModel,
  ConnectionProfile,
  ConnectionSecrets,
  DbAdapter,
  ForeignKeyModel,
  NonQueryResult,
  QueryColumn,
  QueryResult,
  QueryRunOptions,
  RoutineKind,
  RoutineModel,
  RoutineParameterModel,
  SchemaIntrospection,
  SchemaModel,
  TableModel
} from './types';

interface DatabricksSchemaEntry {
  name: string;
  catalogName: string;
  schemaName: string;
  tables: Map<string, TableModel>;
  views: Map<string, TableModel>;
  procedures: RoutineModel[];
  functions: RoutineModel[];
}

interface TableAddress {
  catalogName: string;
  schemaName: string;
  tableName: string;
}

interface RoutineRow {
  [key: string]: unknown;
  routine_catalog?: string;
  routine_schema?: string;
  routine_name?: string;
  specific_name?: string;
  routine_type?: string;
  data_type?: string;
  external_language?: string;
  comment?: string;
}

interface ParameterRow {
  [key: string]: unknown;
  specific_catalog?: string;
  specific_schema?: string;
  specific_name?: string;
  parameter_name?: string;
  parameter_mode?: string;
  data_type?: string;
  ordinal_position?: number | string;
}

const DEFAULT_FETCH_ROWS = 10000;

const TYPE_NAMES: Record<number, string> = {
  0: 'BOOLEAN',
  1: 'TINYINT',
  2: 'SMALLINT',
  3: 'INT',
  4: 'BIGINT',
  5: 'FLOAT',
  6: 'DOUBLE',
  7: 'STRING',
  8: 'TIMESTAMP',
  9: 'BINARY',
  10: 'ARRAY',
  11: 'MAP',
  12: 'STRUCT',
  13: 'UNION',
  14: 'USER_DEFINED',
  15: 'DECIMAL',
  16: 'NULL',
  17: 'DATE',
  18: 'VARCHAR',
  19: 'CHAR',
  20: 'INTERVAL_YEAR_MONTH',
  21: 'INTERVAL_DAY_TIME'
};

export class DatabricksAdapter implements DbAdapter {
  readonly dialect = 'databricks';

  async testConnection(profile: ConnectionProfile, secrets: ConnectionSecrets): Promise<void> {
    await this.withSession(profile, secrets, async (session) => {
      await this.executeAndFetch(session, 'SELECT 1 AS ok', 1, profile);
    });
  }

  async runQuery(
    profile: ConnectionProfile,
    secrets: ConnectionSecrets,
    sql: string,
    options: QueryRunOptions
  ): Promise<QueryResult> {
    return this.withSession(profile, secrets, async (session) => {
      const maxRows = Math.max(1, options.maxRows || DEFAULT_FETCH_ROWS);
      const start = Date.now();
      const operation = await session.executeStatement(sql, this.buildStatementOptions(profile, maxRows, options.timeoutMs));

      try {
        const rows = (await operation.fetchChunk({ maxRows })).map(toJsonSafe) as Record<string, unknown>[];
        const [schema, hasMoreRows] = await Promise.all([
          operation.getSchema(),
          operation.hasMoreRows().catch(() => false)
        ]);
        const elapsedMs = Date.now() - start;

        return {
          columns: this.extractColumns(schema, rows),
          rows,
          rowCount: rows.length,
          elapsedMs,
          warning: hasMoreRows ? `Result limited to the first ${rows.length} rows.` : undefined
        };
      } finally {
        await closeQuietly(operation);
      }
    });
  }

  async executeNonQuery(
    profile: ConnectionProfile,
    secrets: ConnectionSecrets,
    sql: string
  ): Promise<NonQueryResult> {
    return this.withSession(profile, secrets, async (session) => {
      const operation = await session.executeStatement(sql, this.buildStatementOptions(profile));
      try {
        await operation.finished();
        const status = await operation.status().catch(() => undefined);
        return { affectedRows: toNumber((status as Record<string, unknown> | undefined)?.numModifiedRows) ?? null };
      } finally {
        await closeQuietly(operation);
      }
    });
  }

  async introspectSchema(
    profile: ConnectionProfile,
    secrets: ConnectionSecrets
  ): Promise<SchemaIntrospection> {
    return this.withSession(profile, secrets, async (session) => {
      const schemasMap = new Map<string, DatabricksSchemaEntry>();
      const tableAddresses: TableAddress[] = [];
      const catalogNames = await this.getCatalogNames(session, profile);

      for (const catalogName of catalogNames) {
        const schemaNames = await this.getSchemaNames(session, profile, catalogName);
        for (const schemaName of schemaNames) {
          const schema = this.ensureSchema(schemasMap, profile, catalogName, schemaName);
          const tableRows = await this.fetchMetadataRows(() => session.getTables({
            catalogName,
            schemaName,
            tableTypes: ['TABLE', 'VIEW', 'SYSTEM TABLE', 'MATERIALIZED_VIEW']
          }));

          for (const row of tableRows) {
            const tableName = getRowString(row, ['TABLE_NAME', 'table_name', 'tableName']);
            if (!tableName) {
              continue;
            }

            const tableType = getRowString(row, ['TABLE_TYPE', 'table_type', 'tableType']) ?? 'TABLE';
            const comment = getRowString(row, ['REMARKS', 'remarks', 'COMMENT', 'comment']);
            const table: TableModel = {
              name: tableName,
              comment,
              columns: [],
              foreignKeys: []
            };

            if (this.isViewType(tableType)) {
              schema.views.set(tableName, table);
            } else {
              schema.tables.set(tableName, table);
              tableAddresses.push({ catalogName, schemaName, tableName });
            }
          }

          const columnRows = await this.fetchMetadataRows(() => session.getColumns({ catalogName, schemaName }));
          for (const row of columnRows) {
            const tableName = getRowString(row, ['TABLE_NAME', 'table_name', 'tableName']);
            const columnName = getRowString(row, ['COLUMN_NAME', 'column_name', 'columnName']);
            if (!tableName || !columnName) {
              continue;
            }

            const table = schema.tables.get(tableName) ?? schema.views.get(tableName);
            if (!table) {
              continue;
            }

            table.columns.push(this.mapColumn(row, columnName));
          }

          await this.addRoutines(session, schema, catalogName, schemaName);
        }

        await this.addConstraints(session, schemasMap, profile, catalogName);
      }

      await this.addPrimaryKeysFromMetadata(session, schemasMap, profile, tableAddresses);

      const schemas: SchemaModel[] = Array.from(schemasMap.values())
        .sort((a, b) => a.name.localeCompare(b.name))
        .map((schema) => ({
          name: schema.name,
          tables: Array.from(schema.tables.values()).sort((a, b) => a.name.localeCompare(b.name)),
          views: Array.from(schema.views.values()).sort((a, b) => a.name.localeCompare(b.name)),
          procedures: schema.procedures.sort((a, b) => a.name.localeCompare(b.name)),
          functions: schema.functions.sort((a, b) => a.name.localeCompare(b.name))
        }));

      return {
        version: '0.2',
        generatedAt: new Date().toISOString(),
        connectionId: profile.id,
        connectionName: profile.name,
        dialect: 'databricks',
        schemas
      };
    });
  }

  private async withSession<T>(
    profile: ConnectionProfile,
    secrets: ConnectionSecrets,
    callback: (session: any, client: DBSQLClient) => Promise<T>
  ): Promise<T> {
    const client = new DBSQLClient();
    let connectedClient: DBSQLClient | undefined;
    let session: any;

    try {
      connectedClient = await client.connect(this.createConnectionOptions(profile, secrets) as any) as DBSQLClient;
      session = await connectedClient.openSession(this.createOpenSessionRequest(profile));
      return await callback(session, connectedClient);
    } finally {
      if (session) {
        await closeQuietly(session);
      }
      if (connectedClient) {
        await closeQuietly(connectedClient);
      } else {
        await closeQuietly(client);
      }
    }
  }

  private createConnectionOptions(profile: ConnectionProfile, secrets: ConnectionSecrets): Record<string, unknown> {
    const host = normalizeHost(profile.host);
    const path = normalizeHttpPath(profile.httpPath);
    const token = String(secrets.token ?? '').trim();

    if (!host) {
      throw new Error('Databricks workspace host is required.');
    }
    if (!path) {
      throw new Error('Databricks SQL Warehouse HTTP path is required.');
    }
    if (!token) {
      throw new Error('Databricks personal access token is required.');
    }

    return {
      host,
      path,
      token,
      authType: 'access-token',
      userAgentEntry: 'RunQL Databricks Connector'
    };
  }

  private createOpenSessionRequest(profile: ConnectionProfile): Record<string, unknown> {
    const request: Record<string, unknown> = {};
    const catalog = profile.database?.trim();
    const schema = profile.schema?.trim();
    if (catalog) {
      request.initialCatalog = catalog;
    }
    if (schema) {
      request.initialSchema = schema;
    }
    return request;
  }

  private buildStatementOptions(profile: ConnectionProfile, maxRows?: number, timeoutMs?: number): Record<string, unknown> {
    const options: Record<string, unknown> = {
      useCloudFetch: true,
      queryTags: {
        app: 'runql',
        dialect: 'databricks'
      }
    };

    if (maxRows !== undefined) {
      options.maxRows = maxRows;
    }

    const effectiveTimeout = timeoutMs;
    if (effectiveTimeout && effectiveTimeout > 0) {
      options.queryTimeout = Math.ceil(effectiveTimeout / 1000);
    }

    const catalog = profile.database?.trim();
    if (catalog) {
      options.queryTags = { ...(options.queryTags as Record<string, string>), catalog };
    }

    return options;
  }

  private async executeAndFetch(
    session: any,
    sql: string,
    maxRows: number,
    profile: ConnectionProfile
  ): Promise<Record<string, unknown>[]> {
    const operation = await session.executeStatement(sql, this.buildStatementOptions(profile, maxRows));
    try {
      return (await operation.fetchAll({ maxRows })).map(toJsonSafe) as Record<string, unknown>[];
    } finally {
      await closeQuietly(operation);
    }
  }

  private async fetchMetadataRows(operationFactory: () => Promise<any>): Promise<Record<string, unknown>[]> {
    let operation: any;
    try {
      operation = await operationFactory();
      return (await operation.fetchAll()).map(toJsonSafe) as Record<string, unknown>[];
    } catch {
      return [];
    } finally {
      await closeQuietly(operation);
    }
  }

  private async getCatalogNames(session: any, profile: ConnectionProfile): Promise<string[]> {
    const requestedCatalog = profile.database?.trim();
    if (requestedCatalog) {
      return [requestedCatalog];
    }

    const rows = await this.fetchMetadataRows(() => session.getCatalogs());
    const catalogNames = rows
      .map((row) => getRowString(row, ['TABLE_CAT', 'table_cat', 'CATALOG_NAME', 'catalog_name', 'catalog']))
      .filter((catalog): catalog is string => Boolean(catalog))
      .filter((catalog) => catalog.toLowerCase() !== 'system')
      .sort((a, b) => a.localeCompare(b));

    return Array.from(new Set(catalogNames));
  }

  private async getSchemaNames(session: any, profile: ConnectionProfile, catalogName: string): Promise<string[]> {
    const requestedSchema = profile.schema?.trim();
    if (requestedSchema) {
      return [requestedSchema];
    }

    const rows = await this.fetchMetadataRows(() => session.getSchemas({ catalogName }));
    const schemaNames = rows
      .map((row) => getRowString(row, [
        'TABLE_SCHEM',
        'table_schem',
        'TABLE_SCHEMA',
        'table_schema',
        'SCHEMA_NAME',
        'schema_name',
        'databaseName',
        'namespace'
      ]))
      .filter((schema): schema is string => Boolean(schema))
      .filter((schema) => schema.toLowerCase() !== 'information_schema')
      .sort((a, b) => a.localeCompare(b));

    return Array.from(new Set(schemaNames));
  }

  private mapColumn(row: Record<string, unknown>, columnName: string): ColumnModel {
    const typeName = getRowString(row, ['TYPE_NAME', 'type_name', 'DATA_TYPE', 'data_type']) ?? 'UNKNOWN';
    const nullableRaw = getRowValue(row, ['NULLABLE', 'nullable', 'IS_NULLABLE', 'is_nullable']);
    return {
      name: columnName,
      type: typeName,
      nullable: normalizeNullable(nullableRaw),
      comment: getRowString(row, ['REMARKS', 'remarks', 'COMMENT', 'comment'])
    };
  }

  private async addPrimaryKeysFromMetadata(
    session: any,
    schemasMap: Map<string, DatabricksSchemaEntry>,
    profile: ConnectionProfile,
    tableAddresses: TableAddress[]
  ): Promise<void> {
    for (const address of tableAddresses) {
      const schema = schemasMap.get(this.schemaKey(address.catalogName, address.schemaName));
      const table = schema?.tables.get(address.tableName);
      if (!schema || !table || table.primaryKey?.length) {
        continue;
      }

      const rows = await this.fetchMetadataRows(() => session.getPrimaryKeys({
        catalogName: address.catalogName,
        schemaName: address.schemaName,
        tableName: address.tableName
      }));

      const columns = rows
        .map((row) => getRowString(row, ['COLUMN_NAME', 'column_name', 'columnName']))
        .filter((column): column is string => Boolean(column));
      if (columns.length > 0) {
        table.primaryKey = columns;
      }
    }
  }

  private async addConstraints(
    session: any,
    schemasMap: Map<string, DatabricksSchemaEntry>,
    profile: ConnectionProfile,
    catalogName: string
  ): Promise<void> {
    const primaryKeyRows = await this.queryInformationSchema(session, profile, `
      SELECT
        kcu.table_catalog,
        kcu.table_schema,
        kcu.table_name,
        kcu.column_name
      FROM ${this.quoteIdentifier(catalogName)}.information_schema.table_constraints tc
      JOIN ${this.quoteIdentifier(catalogName)}.information_schema.key_column_usage kcu
        ON kcu.constraint_catalog = tc.constraint_catalog
       AND kcu.constraint_schema = tc.constraint_schema
       AND kcu.constraint_name = tc.constraint_name
      WHERE tc.constraint_type = 'PRIMARY KEY'
      ORDER BY kcu.table_schema, kcu.table_name, kcu.ordinal_position
    `);

    for (const row of primaryKeyRows) {
      const schemaName = getRowString(row, ['table_schema']);
      const tableName = getRowString(row, ['table_name']);
      const columnName = getRowString(row, ['column_name']);
      if (!schemaName || !tableName || !columnName) {
        continue;
      }

      const table = schemasMap.get(this.schemaKey(catalogName, schemaName))?.tables.get(tableName);
      if (!table) {
        continue;
      }
      table.primaryKey = table.primaryKey ?? [];
      if (!table.primaryKey.includes(columnName)) {
        table.primaryKey.push(columnName);
      }
    }

    const foreignKeyRows = await this.queryInformationSchema(session, profile, `
      SELECT
        fk.table_schema,
        fk.table_name,
        fk.column_name,
        tc.constraint_name,
        pk.table_schema AS foreign_schema,
        pk.table_name AS foreign_table,
        pk.column_name AS foreign_column
      FROM ${this.quoteIdentifier(catalogName)}.information_schema.table_constraints tc
      JOIN ${this.quoteIdentifier(catalogName)}.information_schema.key_column_usage fk
        ON fk.constraint_catalog = tc.constraint_catalog
       AND fk.constraint_schema = tc.constraint_schema
       AND fk.constraint_name = tc.constraint_name
      JOIN ${this.quoteIdentifier(catalogName)}.information_schema.referential_constraints rc
        ON rc.constraint_catalog = tc.constraint_catalog
       AND rc.constraint_schema = tc.constraint_schema
       AND rc.constraint_name = tc.constraint_name
      JOIN ${this.quoteIdentifier(catalogName)}.information_schema.key_column_usage pk
        ON pk.constraint_catalog = rc.unique_constraint_catalog
       AND pk.constraint_schema = rc.unique_constraint_schema
       AND pk.constraint_name = rc.unique_constraint_name
       AND pk.ordinal_position = fk.position_in_unique_constraint
      WHERE tc.constraint_type = 'FOREIGN KEY'
      ORDER BY fk.table_schema, fk.table_name, tc.constraint_name, fk.ordinal_position
    `);

    for (const row of foreignKeyRows) {
      const schemaName = getRowString(row, ['table_schema']);
      const tableName = getRowString(row, ['table_name']);
      const columnName = getRowString(row, ['column_name']);
      const foreignSchema = getRowString(row, ['foreign_schema']);
      const foreignTable = getRowString(row, ['foreign_table']);
      const foreignColumn = getRowString(row, ['foreign_column']);
      if (!schemaName || !tableName || !columnName || !foreignSchema || !foreignTable || !foreignColumn) {
        continue;
      }

      const table = schemasMap.get(this.schemaKey(catalogName, schemaName))?.tables.get(tableName);
      if (!table) {
        continue;
      }

      const foreignKey: ForeignKeyModel = {
        name: getRowString(row, ['constraint_name']),
        column: columnName,
        foreignSchema: this.displaySchemaName(profile, catalogName, foreignSchema),
        foreignTable,
        foreignColumn
      };
      table.foreignKeys = table.foreignKeys ?? [];
      table.foreignKeys.push(foreignKey);
    }
  }

  private async addRoutines(
    session: any,
    schema: DatabricksSchemaEntry,
    catalogName: string,
    schemaName: string
  ): Promise<void> {
    const routineRows = await this.queryInformationSchema<RoutineRow>(session, undefined, `
      SELECT
        routine_catalog,
        routine_schema,
        routine_name,
        specific_name,
        routine_type,
        data_type,
        external_language,
        comment
      FROM ${this.quoteIdentifier(catalogName)}.information_schema.routines
      WHERE routine_schema = ${quoteLiteral(schemaName)}
      ORDER BY routine_name
    `);

    if (routineRows.length === 0) {
      return;
    }

    const parameterRows = await this.queryInformationSchema<ParameterRow>(session, undefined, `
      SELECT
        specific_catalog,
        specific_schema,
        specific_name,
        parameter_name,
        parameter_mode,
        data_type,
        ordinal_position
      FROM ${this.quoteIdentifier(catalogName)}.information_schema.parameters
      WHERE specific_schema = ${quoteLiteral(schemaName)}
      ORDER BY specific_name, ordinal_position
    `);

    const parametersByRoutine = new Map<string, RoutineParameterModel[]>();
    for (const row of parameterRows) {
      const specificName = row.specific_name;
      if (!specificName) {
        continue;
      }

      const position = toNumber(row.ordinal_position);
      const parameter: RoutineParameterModel = {
        name: row.parameter_name || (position ? `arg${position}` : 'arg'),
        mode: normalizeParameterMode(row.parameter_mode),
        type: row.data_type || undefined,
        position
      };

      const existing = parametersByRoutine.get(specificName) ?? [];
      existing.push(parameter);
      parametersByRoutine.set(specificName, existing);
    }

    for (const row of routineRows) {
      const routineName = row.routine_name;
      if (!routineName) {
        continue;
      }

      const kind: RoutineKind = (row.routine_type ?? '').toUpperCase() === 'PROCEDURE' ? 'procedure' : 'function';
      const routine: RoutineModel = {
        name: routineName,
        kind,
        comment: row.comment,
        returnType: row.data_type || undefined,
        language: row.external_language || undefined,
        schemaQualifiedName: `${schema.name}.${routineName}`,
        parameters: (parametersByRoutine.get(row.specific_name ?? routineName) ?? [])
          .sort((a, b) => (a.position ?? 0) - (b.position ?? 0))
      };
      routine.signature = buildRoutineSignature(routine);

      if (kind === 'procedure') {
        schema.procedures.push(routine);
      } else {
        schema.functions.push(routine);
      }
    }
  }

  private async queryInformationSchema<T extends Record<string, unknown> = Record<string, unknown>>(
    session: any,
    profile: ConnectionProfile | undefined,
    sql: string
  ): Promise<T[]> {
    try {
      const operation = await session.executeStatement(sql, this.buildStatementOptions(profile ?? {} as ConnectionProfile));
      try {
        const rows = await operation.fetchAll();
        return rows.map((row: unknown) => normalizeRecordKeys(toJsonSafe(row) as Record<string, unknown>)) as T[];
      } finally {
        await closeQuietly(operation);
      }
    } catch {
      return [];
    }
  }

  private extractColumns(schema: any, rows: Record<string, unknown>[]): QueryColumn[] {
    const columns = Array.isArray(schema?.columns) ? schema.columns : [];
    if (columns.length > 0) {
      return columns.map((column: any) => {
        const type = this.formatTypeDesc(column.typeDesc);
        return {
          name: String(column.columnName ?? ''),
          type,
          normalizedType: type
        };
      });
    }

    const first = rows[0];
    return first ? Object.keys(first).map((name) => ({ name })) : [];
  }

  private formatTypeDesc(typeDesc: any): string | undefined {
    const entry = Array.isArray(typeDesc?.types) ? typeDesc.types[0] : undefined;
    const primitive = entry?.primitiveEntry;
    if (!primitive) {
      if (entry?.arrayEntry) return 'ARRAY';
      if (entry?.mapEntry) return 'MAP';
      if (entry?.structEntry) return 'STRUCT';
      return undefined;
    }

    const rawType = primitive.type;
    const typeName = TYPE_NAMES[Number(rawType)] ?? String(rawType ?? 'UNKNOWN');
    if (typeName === 'DECIMAL') {
      const precision = getQualifierNumber(primitive, 'precision');
      const scale = getQualifierNumber(primitive, 'scale');
      if (precision !== undefined && scale !== undefined) {
        return `DECIMAL(${precision},${scale})`;
      }
    }
    return typeName;
  }

  private isViewType(tableType: string): boolean {
    return tableType.toUpperCase().includes('VIEW');
  }

  private ensureSchema(
    schemasMap: Map<string, DatabricksSchemaEntry>,
    profile: ConnectionProfile,
    catalogName: string,
    schemaName: string
  ): DatabricksSchemaEntry {
    const key = this.schemaKey(catalogName, schemaName);
    if (!schemasMap.has(key)) {
      schemasMap.set(key, {
        name: this.displaySchemaName(profile, catalogName, schemaName),
        catalogName,
        schemaName,
        tables: new Map(),
        views: new Map(),
        procedures: [],
        functions: []
      });
    }
    return schemasMap.get(key)!;
  }

  private schemaKey(catalogName: string, schemaName: string): string {
    return `${catalogName}\u0000${schemaName}`;
  }

  private displaySchemaName(profile: ConnectionProfile, catalogName: string, schemaName: string): string {
    const requestedCatalog = profile.database?.trim();
    return requestedCatalog && requestedCatalog === catalogName ? schemaName : `${catalogName}.${schemaName}`;
  }

  private quoteIdentifier(identifier: string): string {
    return `\`${identifier.replace(/`/g, '``')}\``;
  }
}

function normalizeHost(host: string | undefined): string {
  return String(host ?? '')
    .trim()
    .replace(/^https?:\/\//i, '')
    .split(/[/?#]/)[0]
    .replace(/\.$/, '');
}

function normalizeHttpPath(path: string | undefined): string {
  const value = String(path ?? '').trim();
  if (!value) {
    return '';
  }
  return value.startsWith('/') ? value : `/${value}`;
}

async function closeQuietly(target: { close: () => Promise<unknown> } | undefined): Promise<void> {
  if (!target) {
    return;
  }
  try {
    await target.close();
  } catch {
    // Closing is best effort after failed query or metadata operations.
  }
}

function toJsonSafe(value: unknown): unknown {
  if (value === null || value === undefined) return value;
  if (typeof value === 'bigint') {
    return value <= Number.MAX_SAFE_INTEGER && value >= Number.MIN_SAFE_INTEGER
      ? Number(value)
      : value.toString();
  }
  if (value instanceof Date) {
    return isNaN(value.getTime()) ? null : value.toISOString();
  }
  if (Buffer.isBuffer(value)) {
    return value.toString('base64');
  }
  if (Array.isArray(value)) {
    return value.map(toJsonSafe);
  }
  if (typeof value === 'object') {
    const record = value as Record<string, unknown>;
    if (typeof (record as { toNumber?: unknown }).toNumber === 'function') {
      const numeric = (record as { toNumber: () => number }).toNumber();
      return Number.isSafeInteger(numeric) ? numeric : String(value);
    }

    const out: Record<string, unknown> = {};
    for (const [key, nested] of Object.entries(record)) {
      out[key] = toJsonSafe(nested);
    }
    return out;
  }
  return value;
}

function normalizeRecordKeys(row: Record<string, unknown>): Record<string, unknown> {
  const normalized: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(row)) {
    normalized[key.toLowerCase()] = value;
  }
  return normalized;
}

function getRowString(row: Record<string, unknown>, keys: string[]): string | undefined {
  const value = getRowValue(row, keys);
  if (typeof value === 'string') return value.trim() || undefined;
  if (value === null || value === undefined) return undefined;
  return String(value);
}

function getRowValue(row: Record<string, unknown>, keys: string[]): unknown {
  for (const key of keys) {
    if (row[key] !== undefined) {
      return row[key];
    }
  }

  const lowerKeys = new Set(keys.map((key) => key.toLowerCase()));
  for (const [key, value] of Object.entries(row)) {
    if (lowerKeys.has(key.toLowerCase())) {
      return value;
    }
  }

  return undefined;
}

function normalizeNullable(value: unknown): boolean | undefined {
  if (value === undefined || value === null) {
    return undefined;
  }
  if (typeof value === 'boolean') {
    return value;
  }
  if (typeof value === 'number') {
    return value !== 0;
  }

  const text = String(value).trim().toUpperCase();
  if (text === 'YES' || text === 'TRUE' || text === 'NULLABLE') return true;
  if (text === 'NO' || text === 'FALSE' || text === 'NOT NULL' || text === 'REQUIRED') return false;
  return undefined;
}

function getQualifierNumber(primitive: any, key: string): number | undefined {
  const qualifier = primitive?.typeQualifiers?.qualifiers?.[key];
  return toNumber(qualifier?.i32Value ?? qualifier?.i64Value ?? qualifier?.stringValue);
}

function toNumber(value: unknown): number | undefined {
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  if (typeof value === 'bigint') {
    return value <= BigInt(Number.MAX_SAFE_INTEGER) && value >= BigInt(Number.MIN_SAFE_INTEGER)
      ? Number(value)
      : undefined;
  }
  if (typeof value === 'string' && value.trim()) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : undefined;
  }
  if (value && typeof value === 'object' && typeof (value as { toNumber?: unknown }).toNumber === 'function') {
    const parsed = (value as { toNumber: () => number }).toNumber();
    return Number.isFinite(parsed) ? parsed : undefined;
  }
  return undefined;
}

function quoteLiteral(value: string): string {
  return `'${value.replace(/'/g, "''")}'`;
}

function normalizeParameterMode(modeRaw: string | undefined): 'in' | 'out' | 'inout' | 'variadic' | 'return' | undefined {
  const value = (modeRaw ?? '').trim().toUpperCase();
  if (value === 'IN') return 'in';
  if (value === 'OUT') return 'out';
  if (value === 'INOUT') return 'inout';
  if (value === 'VARIADIC') return 'variadic';
  if (value === 'RETURN') return 'return';
  return undefined;
}

function buildRoutineSignature(routine: RoutineModel): string {
  const args = (routine.parameters ?? [])
    .filter((parameter) => parameter.mode !== 'return')
    .map((parameter) => {
      const modePrefix = parameter.mode ? `${parameter.mode.toUpperCase()} ` : '';
      const typeSuffix = parameter.type ? ` ${parameter.type}` : '';
      return `${modePrefix}${parameter.name}${typeSuffix}`.trim();
    })
    .join(', ');
  return `${routine.name}(${args})`;
}
