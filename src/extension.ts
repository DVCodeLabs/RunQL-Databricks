import * as vscode from 'vscode';
import { RunQLExtensionApi } from './types';
import { databricksProvider } from './provider';
import { DatabricksAdapter } from './databricksAdapter';

export async function activate(context: vscode.ExtensionContext) {
  const core = vscode.extensions.getExtension<RunQLExtensionApi>('RunQL-VSCode-Extension.runql');
  if (!core) {
    vscode.window.showWarningMessage('RunQL Databricks Connector requires RunQL-VSCode-Extension.runql.');
    return;
  }

  const api = await core.activate();
  if (!api || typeof api.registerProvider !== 'function' || typeof api.registerAdapter !== 'function') {
    vscode.window.showWarningMessage('RunQL core API is unavailable. Update RunQL and try again.');
    return;
  }

  context.subscriptions.push(
    api.registerProvider(databricksProvider),
    api.registerAdapter('databricks', () => new DatabricksAdapter())
  );
}

export function deactivate() {
  // no-op
}
