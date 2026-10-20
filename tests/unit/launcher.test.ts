import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { spawn, ChildProcess } from 'child_process';
import { CodesysLauncher } from '../../src/launcher';

// These tests use the mock watcher directly (not CODESYS)
// They validate the launcher's IPC integration behavior

describe('CodesysLauncher', () => {
  // launch() scans and prunes %TEMP%\codesys-mcp-persistent; keep it off the real one.
  const saved = { TEMP: process.env.TEMP, TMP: process.env.TMP, TMPDIR: process.env.TMPDIR };
  let fakeTemp: string;
  beforeEach(() => {
    fakeTemp = fs.mkdtempSync(path.join(os.tmpdir(), 'cmcp-launcher-'));
    process.env.TEMP = process.env.TMP = process.env.TMPDIR = fakeTemp;
  });
  afterEach(() => {
    for (const [k, v] of Object.entries(saved)) {
      if (v === undefined) delete process.env[k];
      else process.env[k] = v;
    }
    fs.rmSync(fakeTemp, { recursive: true, force: true });
  });

  it('rejects launch when CODESYS exe not found', async () => {
    const launcher = new CodesysLauncher({
      codesysPath: 'C:\\nonexistent\\CODESYS.exe',
      profileName: 'Test Profile',
      workspaceDir: os.tmpdir(),
    });

    await expect(launcher.launch()).rejects.toThrow(/not found/);
    const status = launcher.getStatus();
    expect(status.state).toBe('error');
  });

  it('getStatus reports stopped initially', () => {
    const launcher = new CodesysLauncher({
      codesysPath: 'C:\\nonexistent\\CODESYS.exe',
      profileName: 'Test Profile',
      workspaceDir: os.tmpdir(),
    });

    const status = launcher.getStatus();
    expect(status.state).toBe('stopped');
    expect(status.pid).toBeNull();
    expect(status.sessionId).toBeNull();
  });

  it('isRunning returns false when not launched', () => {
    const launcher = new CodesysLauncher({
      codesysPath: 'C:\\nonexistent\\CODESYS.exe',
      profileName: 'Test Profile',
      workspaceDir: os.tmpdir(),
    });

    expect(launcher.isRunning()).toBe(false);
  });

  it('executeScript rejects when not ready', async () => {
    const launcher = new CodesysLauncher({
      codesysPath: 'C:\\nonexistent\\CODESYS.exe',
      profileName: 'Test Profile',
      workspaceDir: os.tmpdir(),
    });

    await expect(launcher.executeScript('print("hi")')).rejects.toThrow(/state is 'stopped'/);
  });
});
