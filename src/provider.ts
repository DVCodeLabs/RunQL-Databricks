import { DPProviderDescriptor } from './types';

export const databricksProvider: DPProviderDescriptor = {
  providerId: 'databricks',
  displayName: 'Databricks',
  dialect: 'databricks',
  formSchema: {
    fields: [
      {
        key: 'host',
        label: 'Workspace Host',
        type: 'text',
        tab: 'connection',
        storage: 'profile',
        required: true,
        placeholder: 'adb-1234567890123456.7.azuredatabricks.net',
        description: 'Databricks workspace hostname without https://.',
        width: 'full'
      },
      {
        key: 'httpPath',
        label: 'SQL Warehouse HTTP Path',
        type: 'text',
        tab: 'connection',
        storage: 'profile',
        required: true,
        placeholder: '/sql/2.0/warehouses/xxxxxxxxxxxxxxxx',
        width: 'full'
      },
      {
        key: 'database',
        label: 'Catalog (Optional)',
        type: 'text',
        tab: 'connection',
        storage: 'profile',
        placeholder: 'main',
        description: 'Sets the session catalog and scopes introspection when provided.',
        width: 'half'
      },
      {
        key: 'schema',
        label: 'Schema (Optional)',
        type: 'text',
        tab: 'connection',
        storage: 'profile',
        placeholder: 'default',
        description: 'Sets the session schema and scopes introspection when provided.',
        width: 'half'
      },
      {
        key: 'authMode',
        label: 'Authentication Mode',
        type: 'select',
        tab: 'auth',
        storage: 'profile',
        defaultValue: 'pat',
        options: [
          { value: 'pat', label: 'Personal Access Token' }
        ],
        width: 'full'
      },
      {
        key: 'token',
        label: 'Personal Access Token',
        type: 'password',
        tab: 'auth',
        storage: 'secrets',
        required: true,
        placeholder: 'dapi...',
        width: 'full'
      }
    ]
  },
  supports: {
    ssl: true,
    oauth: false,
    keypair: false,
    introspection: true,
    cancellation: true
  }
};
