/**
 * CODESYS launcher — spawns CODESYS with UI and watcher script,
 * tracks process lifecycle, delegates to IPC for command execution.
 */

import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { spawn, ChildProcess } from 'child_process';
import { v4 as uuidv4 } from 'uuid';
import { LauncherConfig, LauncherStatus, CodesysState, IpcResult, ScriptExecutor } from './types';
import { IpcClient, DEFAULT_IPC_CONFIG } from './ipc';
import { ScriptManager } from './script-manager';
import { launcherLog } from './logger';

const SESSION_DIR_PREFIX = 'codesys-mcp-persistent';
// Cold first-launch of CODESYS V3.5 SP16 Patch 5 takes ~120s on a bench PC
// before the watcher writes ready.signal. Default was 60s, which landed us
// in 'error' state every time. Override via CODESYS_MCP_READY_TIMEOUT_MS.
const READY_TIMEOUT_MS = Number(process.env.CODESYS_MCP_READY_TIMEOUT_MS) > 0
  ? Number(process.env.CODESYS_MCP_READY_TIMEOUT_MS)
  : 180_000;
const READY_POLL_MS = 500;
const SHUTDOWN_WAIT_MS = 5_000;
const HEALTH_CHECK_INTERVAL_MS = 5_000;

// Ownership lease. Every server writes owner.json into the session dir it drives and
// refreshes it every few seconds, so other servers on the PC (other client sessions) can
// tell a live owner from an orphan left by a restart.
const LEASE_FILE = 'owner.json';
const LEASE_TTL_MS = 30_000;
// A session dir with no live CODESYS and no live owner is deleted once this old.
const STALE_DIR_AGE_MS = 60 * 60_000;
// Identifies this server process; a PID alone is recycled by Windows.
const SERVER_ID = uuidv4();

export interface SessionOwner {
  /** Host session id (CLAUDE_CODE_HOST_SESSION_ID / CLAUDE_CODE_SESSION_ID) or CODESYS_MCP_OWNER; null when unknown. */
  tag: string | null;
  serverId: string | null;
  serverPid: number;
  parentPid: number;
  heartbeat: number;
}

export interface Identity {
  tag: string | null;
  serverId: string;
  serverPid: number;
  parentPid: number;
}

export type Ownership =
  | 'self'          // this server process
  | 'same-session'  // another server of this client session (reconnect / app restart)
  | 'other-live'    // a live server of another session - never adopt implicitly
  | 'orphan'        // its server is gone, and it was another session's
  | 'unowned';      // no owner.json: hand-launched watcher or a pre-lease server

export interface SessionInfo {
  id: string;
  dir: string;
  pid: number | null;
  codesysAlive: boolean;
  pythonVersion: string | null;
  readyMtime: number | null;
  owner: SessionOwner | null;
  ownership: Ownership;
  leaseFresh: boolean;
  newestMtime: number;
}

export interface LaunchOutcome {
  action: 'adopted' | 'spawned' | 'recovered' | 'already-running';
  pid: number | null;
  sessionId: string | null;
  /** Live instances on this PC that were left alone. */
  others: SessionInfo[];
}

/** pid -> process start (epoch ms) and main window title, or null when the listing failed. */
export type CodesysSnapshot = Map<number, { start: number; title: string }>;

export function currentOwnerTag(): string | null {
  return process.env.CODESYS_MCP_OWNER
    || process.env.CLAUDE_CODE_HOST_SESSION_ID
    || process.env.CLAUDE_CODE_SESSION_ID
    || null;
}

/** EPERM means the process exists but belongs to another integrity level (elevated). */
function isPidAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (err) {
    return (err as NodeJS.ErrnoException)?.code === 'EPERM';
  }
}

export function classifyOwnership(
  owner: SessionOwner | null,
  me: { tag: string | null; serverId: string },
  now: number,
  serverAlive: (pid: number) => boolean = isPidAlive,
): { ownership: Ownership; leaseFresh: boolean } {
  if (!owner) return { ownership: 'unowned', leaseFresh: false };
  const leaseFresh = now - owner.heartbeat < LEASE_TTL_MS && serverAlive(owner.serverPid);
  if (owner.serverId !== null && owner.serverId === me.serverId) return { ownership: 'self', leaseFresh };
  if (me.tag && owner.tag === me.tag) return { ownership: 'same-session', leaseFresh };
  return { ownership: leaseFresh ? 'other-live' : 'orphan', leaseFresh };
}

function mtimeOf(p: string): number {
  try { return fs.statSync(p).mtimeMs; } catch { return 0; }
}

/**
 * Read every session dir under `root`. Pure apart from the filesystem; the CODESYS and
 * server liveness probes are injected so tests can fake them. `isCodesys` gets the
 * ready.signal mtime so it can reject a PID recycled after that signal was written.
 */
export function scanSessionsIn(
  root: string,
  me: { tag: string | null; serverId: string },
  isCodesys: (pid: number, readyMtime: number | null) => boolean,
  now: number = Date.now(),
  serverAlive: (pid: number) => boolean = isPidAlive,
): SessionInfo[] {
  if (!fs.existsSync(root)) return [];
  const out: SessionInfo[] = [];
  for (const ent of fs.readdirSync(root, { withFileTypes: true })) {
    // Junctions (the old '<guid>.hold' sharing trick) are not directories here; leave them.
    if (!ent.isDirectory()) continue;
    const dir = path.join(root, ent.name);
    // Commands and results land in subdirs and the watcher appends to its log, so the
    // root dir's own mtime alone says nothing about activity.
    const newest = ['', 'commands', 'results', 'watcher.log', 'ready.signal', LEASE_FILE]
      .map((p) => mtimeOf(path.join(dir, p)));
    if (newest[0] === 0) continue;
    const newestMtime = Math.max(...newest);

    let pid: number | null = null;
    let pythonVersion: string | null = null;
    let readyMtime: number | null = null;
    const sigPath = path.join(dir, 'ready.signal');
    try {
      readyMtime = fs.statSync(sigPath).mtimeMs;
      const sig = JSON.parse(fs.readFileSync(sigPath, 'utf-8'));
      if (typeof sig.pid === 'number') pid = sig.pid;
      if (typeof sig.python_version === 'string') pythonVersion = sig.python_version;
    } catch { /* no or malformed ready.signal */ }

    let owner: SessionOwner | null = null;
    try {
      const o = JSON.parse(fs.readFileSync(path.join(dir, LEASE_FILE), 'utf-8'));
      if (typeof o.serverPid === 'number' && typeof o.heartbeat === 'number') {
        owner = {
          tag: typeof o.tag === 'string' ? o.tag : null,
          serverId: typeof o.serverId === 'string' ? o.serverId : null,
          serverPid: o.serverPid,
          parentPid: typeof o.parentPid === 'number' ? o.parentPid : 0,
          heartbeat: o.heartbeat,
        };
      }
    } catch { /* no lease */ }

    const { ownership, leaseFresh } = classifyOwnership(owner, me, now, serverAlive);
    out.push({
      id: ent.name,
      dir,
      pid,
      codesysAlive: pid !== null && isCodesys(pid, readyMtime),
      pythonVersion,
      readyMtime,
      owner,
      ownership,
      leaseFresh,
      newestMtime,
    });
  }
  return out;
}

/**
 * The session a plain launch_codesys may attach to without being told: one this client
 * session owned whose server is gone (an MCP reconnect or an app restart). With no
 * session identity at all, an orphan from the same parent process. Never another
 * session's instance, live or orphaned, never an unowned one, and never one another
 * live server of this session still drives - those need an explicit `adopt`.
 */
export function pickImplicitAdoption(
  sessions: SessionInfo[],
  me: { tag: string | null; parentPid: number },
): SessionInfo | null {
  const live = sessions.filter((s) => s.codesysAlive);
  const mine = live.filter((s) =>
    s.ownership === 'self' ||
    (s.ownership === 'same-session' && !s.leaseFresh) ||
    (me.tag === null && s.ownership === 'orphan' && s.owner !== null &&
      s.owner.tag === null && s.owner.parentPid === me.parentPid));
  if (mine.length === 0) return null;
  mine.sort((a, b) => (b.readyMtime ?? 0) - (a.readyMtime ?? 0));
  return mine[0];
}

/** Dirs safe to delete: CODESYS gone, no live owner, idle for STALE_DIR_AGE_MS. */
export function selectStaleSessions(sessions: SessionInfo[], now: number): SessionInfo[] {
  return sessions.filter((s) =>
    !s.codesysAlive && !s.leaseFresh && s.ownership !== 'self' &&
    now - s.newestMtime > STALE_DIR_AGE_MS);
}

export class CodesysLauncher implements ScriptExecutor {
  private config: LauncherConfig;
  private state: CodesysState = 'stopped';
  private pid: number | null = null;
  private sessionId: string | null = null;
  private ipcDir: string | null = null;
  private ipcClient: IpcClient | null = null;
  private process: ChildProcess | null = null;
  private startedAt: number | null = null;
  private lastError: string | null = null;
  private healthInterval: ReturnType<typeof setInterval> | null = null;
  private leaseInterval: ReturnType<typeof setInterval> | null = null;
  private stateChangeCallbacks: Array<(state: CodesysState) => void> = [];

  constructor(config: LauncherConfig) {
    this.config = config;
  }

  /**
   * Verify a PID is actually a CODESYS process.
   *
   * `process.kill(pid, 0)` only proves *some* process holds the PID. Windows
   * recycles PIDs aggressively, so a dead CODESYS whose PID has been reused
   * would otherwise be adopted as a live session - and, worse, force-killed on
   * shutdown, terminating whatever unrelated program now owns that PID.
   * Observed: a recycled CODESYS PID that by then belonged to the MCP host's
   * own Node process. Fails closed (returns false) so a failed probe never
   * authorises a kill.
   */
  private isCodesysProcess(pid: number): boolean {
    if (!isPidAlive(pid)) return false; // no process holds this PID
    if (process.platform !== 'win32') return true;
    try {
      const { execSync } = require('child_process');
      const out: string = execSync(`tasklist /FI "PID eq ${pid}" /FO CSV /NH`, {
        timeout: 5_000,
        encoding: 'utf-8',
      });
      const line = String(out).trim().toLowerCase();
      // tasklist prints `INFO: No tasks are running...` (unquoted) when absent
      if (!line.startsWith('"')) return false;
      const expected = path.basename(this.config.codesysPath).toLowerCase();
      return line.includes(expected);
    } catch {
      return false;
    }
  }

  private identity(): Identity {
    return { tag: currentOwnerTag(), serverId: SERVER_ID, serverPid: process.pid, parentPid: process.ppid };
  }

  /**
   * One listing of every CODESYS process: start time and window title. Null when the
   * listing failed - callers must then not act on "this CODESYS is gone".
   */
  private codesysSnapshot(): CodesysSnapshot | null {
    if (process.platform !== 'win32') return null;
    try {
      const { execFileSync } = require('child_process');
      const exe = path.basename(this.config.codesysPath).replace(/'/g, "''");
      const ps =
        '[Console]::OutputEncoding = [Text.Encoding]::UTF8; ' +
        `ConvertTo-Json -Compress -InputObject @(Get-CimInstance Win32_Process -Filter "Name='${exe}'" | ` +
        'ForEach-Object { $p = Get-Process -Id $_.ProcessId -ErrorAction SilentlyContinue; ' +
        '[pscustomobject]@{ pid = [int]$_.ProcessId; ' +
        'start = [long]([DateTimeOffset]$_.CreationDate).ToUnixTimeMilliseconds(); ' +
        'title = [string]$p.MainWindowTitle } })';
      const out: string = execFileSync('powershell.exe', ['-NoProfile', '-NonInteractive', '-Command', ps], {
        timeout: 20_000,
        encoding: 'utf-8',
        windowsHide: true,
      });
      const list = JSON.parse(String(out).trim() || '[]');
      const snap: CodesysSnapshot = new Map();
      for (const e of Array.isArray(list) ? list : [list]) {
        if (typeof e?.pid === 'number') snap.set(e.pid, { start: Number(e.start) || 0, title: e.title ?? '' });
      }
      return snap;
    } catch (err) {
      launcherLog.warn(`CODESYS process listing failed: ${err}`);
      return null;
    }
  }

  private scan(): { sessions: SessionInfo[]; snapshot: CodesysSnapshot | null } {
    const snapshot = this.codesysSnapshot();
    // A PID that started after its ready.signal was written is a recycled one.
    const isCodesys = (pid: number, readyMtime: number | null) => snapshot
      ? snapshot.has(pid) && (readyMtime === null || snapshot.get(pid)!.start <= readyMtime + 2_000)
      : this.isCodesysProcess(pid);
    try {
      const sessions = scanSessionsIn(path.join(os.tmpdir(), SESSION_DIR_PREFIX), this.identity(), isCodesys);
      return { sessions, snapshot };
    } catch (err) {
      launcherLog.warn(`Session scan failed: ${err}`);
      return { sessions: [], snapshot: null };
    }
  }

  /** Every session dir on this PC, classified against this server. */
  scanSessions(): SessionInfo[] {
    return this.scan().sessions;
  }

  /**
   * Delete session dirs with no live CODESYS, no live owner and no activity for an hour.
   * Only on a successful process listing: a failed probe must never delete a live dir.
   */
  private removeStaleSessions(sessions: SessionInfo[], snapshot: CodesysSnapshot | null): SessionInfo[] {
    if (!snapshot) return sessions;
    const stale = new Set(selectStaleSessions(sessions, Date.now()));
    return sessions.filter((s) => {
      if (!stale.has(s)) return true;
      try {
        fs.rmSync(s.dir, { recursive: true, force: true });
        launcherLog.info(`Removed stale session dir ${s.id}`);
        return false;
      } catch (err) {
        launcherLog.warn(`Could not remove stale session dir ${s.id}: ${err}`);
        return true;
      }
    });
  }

  /** Record (or refresh) this server as the owner of the session it drives. */
  private writeLease(): void {
    if (!this.ipcDir) return;
    const lease: SessionOwner = { ...this.identity(), heartbeat: Date.now() };
    const target = path.join(this.ipcDir, LEASE_FILE);
    try {
      fs.writeFileSync(target + '.tmp', JSON.stringify(lease), 'utf-8');
      fs.renameSync(target + '.tmp', target);
    } catch (err) {
      launcherLog.warn(`Could not write ${LEASE_FILE}: ${err}`);
    }
  }

  /** Keep the lease fresh in every state while this server holds a session dir. */
  private startLease(): void {
    this.writeLease();
    if (this.leaseInterval) return;
    this.leaseInterval = setInterval(() => this.writeLease(), HEALTH_CHECK_INTERVAL_MS);
  }

  /**
   * The client is gone: stop the heartbeat and mark the lease expired, so this
   * session's next server re-attaches at once. CODESYS keeps running.
   */
  releaseLease(): void {
    this.stopLease();
    if (!this.ipcDir) return;
    try {
      const lease: SessionOwner = { ...this.identity(), heartbeat: 0 };
      fs.writeFileSync(path.join(this.ipcDir, LEASE_FILE), JSON.stringify(lease), 'utf-8');
    } catch { /* best effort */ }
  }

  private stopLease(): void {
    if (this.leaseInterval) {
      clearInterval(this.leaseInterval);
      this.leaseInterval = null;
    }
  }

  private async attach(s: SessionInfo): Promise<void> {
    launcherLog.info(`Adopting session ${s.id}: PID ${s.pid} (${s.ownership})`);
    // A CODESYS this server spawned earlier (a timed-out launch) stops being tracked.
    if (this.process) {
      this.process.removeAllListeners('exit');
      this.process = null;
    }
    this.sessionId = s.id;
    this.ipcDir = s.dir;
    this.ipcClient = new IpcClient({ baseDir: this.ipcDir, ...DEFAULT_IPC_CONFIG });
    await this.ipcClient.ensureDirectories();
    this.pid = s.pid;
    this.startedAt = s.readyMtime;
    this.lastError = null;
    this.startLease();
    this.setState('ready');
    this.startHealthMonitor();
  }

  /**
   * Launch CODESYS with UI and watcher script, or attach to a running one.
   *
   * `overrides.additionalFolder` lets a caller pick the AdditionalFolder per
   * launch instead of per server process, so one MCP config can serve several
   * projects with different pinned library sets. The configured value stays
   * the fallback when no override is supplied.
   *
   * `adopt` (session id or CODESYS PID) attaches to that instance; one a live server
   * of another session owns needs `force`. `fresh` always spawns a new instance.
   */
  async launch(overrides?: {
    additionalFolder?: string;
    adopt?: string;
    fresh?: boolean;
    force?: boolean;
  }): Promise<LaunchOutcome> {
    if (this.state === 'ready' || this.state === 'launching') {
      if (overrides?.adopt) {
        throw new Error(`Already attached to PID ${this.pid} (session ${this.sessionId}); shutdown_codesys first`);
      }
      launcherLog.warn(`Cannot launch: state is ${this.state}`);
      return { action: 'already-running', pid: this.pid, sessionId: this.sessionId, others: [] };
    }

    const additionalFolder = overrides?.additionalFolder ?? this.config.additionalFolder;
    const scanned = this.scan();
    const sessions = this.removeStaleSessions(scanned.sessions, scanned.snapshot);
    const othersThan = (id: string | null) => sessions.filter((s) => s.codesysAlive && s.id !== id);

    if (overrides?.adopt) {
      const want = overrides.adopt.trim().replace(/^ide-/i, '');
      const target = sessions.find((s) => s.id === want || String(s.pid) === want);
      if (!target || !target.codesysAlive) {
        throw new Error(`No live CODESYS session matches '${overrides.adopt}' (give a session id or a CODESYS PID from get_codesys_status)`);
      }
      if (target.ownership === 'other-live' && !overrides.force) {
        throw new Error(
          `Session ${target.id} (PID ${target.pid}) is driven by another live MCP server ` +
          `(server PID ${target.owner?.serverPid}, owner ${target.owner?.tag ?? 'unknown'}). ` +
          `Pass force=true only if that session is idle - two servers on one IDE interleave scripts.`
        );
      }
      await this.attach(target);
      return { action: 'adopted', pid: this.pid, sessionId: this.sessionId, others: othersThan(target.id) };
    }

    // Re-attach to an instance this client session already owned. This closes out the
    // "orphan CODESYS on MCP restart" problem - without it, a /mcp reconnect spawns a
    // second CODESYS on top of the live one and they fight for project lockfiles. Only on
    // a genuinely fresh launcher, or after a failed launch whose CODESYS is gone. Another
    // session's instance is never taken implicitly (that used to hijack it: the newest
    // live ready.signal won), nor is an unowned hand-launched one - see `adopt`.
    const deadAfterError = this.state === 'error' && (this.pid === null || !this.isCodesysProcess(this.pid));
    if (!overrides?.fresh && ((this.pid === null && this.state === 'stopped') || deadAfterError)) {
      const pick = pickImplicitAdoption(
        sessions.filter((s) => !s.pythonVersion || !this.config.profileName ||
          s.pythonVersion.includes(this.config.profileName)),
        this.identity(),
      );
      if (pick) {
        await this.attach(pick);
        return { action: 'adopted', pid: this.pid, sessionId: this.sessionId, others: othersThan(pick.id) };
      }
      // A failed launch next to a hand-started CODESYS with its own watcher: name it
      // rather than spawning a second IDE beside it.
      const handmade = sessions.filter((s) => s.codesysAlive && s.ownership === 'unowned');
      if (deadAfterError && handmade.length > 0) {
        throw new Error(
          'A CODESYS with an MCP watcher but no owner is running: ' +
          handmade.map((s) => `PID ${s.pid} (session ${s.id})`).join(', ') +
          '. Attach with adopt=<session or PID>, or pass fresh=true to start another instance.'
        );
      }
    }

    // Recovery path: we previously timed out, but the CODESYS process the
    // launcher spawned is still alive and its watcher has now written
    // ready.signal (or is about to). Re-attach rather than spawning a fresh
    // CODESYS on top — two instances would fight for project lockfiles. Cold
    // SP16 P5 launches frequently exceed the historical 60s budget, so this
    // path also covers the "MCP gave up too early" case independently of the
    // timeout bump above.
    if (this.state === 'error' && this.pid !== null && this.ipcDir && this.ipcClient) {
      const stillAlive = this.isRunning();
      if (stillAlive) {
        const readyNow = await this.ipcClient.isReady();
        if (readyNow) {
          launcherLog.info(`Recovery: live PID ${this.pid} watcher already ready; attaching`);
          this.lastError = null;
          if (this.startedAt === null) this.startedAt = Date.now();
          this.writeLease();
          this.setState('ready');
          this.startHealthMonitor();
          return { action: 'recovered', pid: this.pid, sessionId: this.sessionId, others: othersThan(this.sessionId) };
        }
        launcherLog.warn(`Recovery: PID ${this.pid} alive but watcher not ready yet — polling without respawn`);
        this.setState('launching');
        const recoverStart = Date.now();
        while (Date.now() - recoverStart < READY_TIMEOUT_MS) {
          if (await this.ipcClient.isReady()) {
            this.lastError = null;
            if (this.startedAt === null) this.startedAt = Date.now();
            this.writeLease();
            this.setState('ready');
            launcherLog.info('CODESYS watcher is ready (recovery path)');
            this.startHealthMonitor();
            return { action: 'recovered', pid: this.pid, sessionId: this.sessionId, others: othersThan(this.sessionId) };
          }
          await this.sleep(READY_POLL_MS);
        }
        this.lastError = `Recovery: PID ${this.pid} still no ready.signal after ${READY_TIMEOUT_MS}ms`;
        this.setState('error');
        throw new Error(this.lastError);
      }
      // Recorded PID is dead — clear it and fall through to a fresh spawn.
      launcherLog.info(`Recovery: PID ${this.pid} no longer alive; spawning fresh`);
      this.pid = null;
      this.process = null;
    }

    // Validate CODESYS exe exists
    if (!fs.existsSync(this.config.codesysPath)) {
      const err = `CODESYS executable not found: ${this.config.codesysPath}`;
      this.setState('error');
      this.lastError = err;
      throw new Error(err);
    }

    // Optional: kill any pre-existing CODESYS.exe before launching. This is
    // only useful in dev to clean up after an MCP server restart that left
    // the old CODESYS detached and holding a project lock. It is OFF by
    // default because killing an unrelated CODESYS instance the user is
    // working in would lose unsaved work. Opt in with --kill-existing-codesys.
    if (this.config.killExistingCodesys === true && process.platform === 'win32') {
      try {
        const { execSync } = require('child_process');
        const exeBase = path.basename(this.config.codesysPath);
        try {
          execSync(`taskkill /F /T /IM "${exeBase}"`, { timeout: 10_000, stdio: 'ignore' });
          launcherLog.info(`Killed pre-existing ${exeBase} processes (opted-in via --kill-existing-codesys).`);
          await this.sleep(2_000);
        } catch {
          // Most common failure: no process found. That's the normal case.
        }
      } catch (killErr) {
        launcherLog.warn(`Pre-launch kill skipped: ${killErr}`);
      }
    }

    this.setState('launching');
    this.sessionId = uuidv4();
    this.ipcDir = path.join(os.tmpdir(), SESSION_DIR_PREFIX, this.sessionId);

    launcherLog.info(`Session ${this.sessionId} — IPC dir: ${this.ipcDir}`);

    // Create IPC client and directories
    this.ipcClient = new IpcClient({
      baseDir: this.ipcDir,
      ...DEFAULT_IPC_CONFIG,
    });
    await this.ipcClient.ensureDirectories();
    // Claim the dir before CODESYS exists, so no other server takes it as an orphan.
    this.startLease();

    // Prepare watcher script with interpolated IPC path. ScriptManager.
    // interpolate() now Python-escapes the value, so no manual pre-escape.
    const scriptManager = new ScriptManager();
    const watcherTemplate = scriptManager.loadTemplate('watcher');
    const watcherContent = scriptManager.interpolate(watcherTemplate, {
      IPC_BASE_DIR: this.ipcDir,
    });

    // Write interpolated watcher to IPC directory
    const watcherPath = path.join(this.ipcDir, 'watcher.py');
    fs.writeFileSync(watcherPath, watcherContent, 'utf-8');

    // Build CODESYS args. Pass argv directly (no shell) so this.process.pid
    // is the real CODESYS PID rather than a wrapping cmd.exe shell PID.
    // Values are pre-quoted and passed verbatim (see windowsVerbatimArguments below).
    const codesysArgs = [
      // Quote the value like headless.ts does: an unquoted name with spaces reaches
      // CODESYS as `--profile=CODESYS V3.5 SP19 Patch 7` and the IDE falls back to the
      // 'Select Profile' dialog, which blocks --runscript until someone clicks Continue.
      `--profile="${this.config.profileName}"`,
    ];
    if (additionalFolder) {
      // Quoted for the same reason as --profile, and as the CODESYS Installer passes it:
      // AdditionalFolders usually live under "C:\Program Files\...", and an unquoted value
      // with spaces reached CODESYS mangled - the launch crashed in the plug-in cache rebuild.
      codesysArgs.push(`--additionalfolder="${additionalFolder}"`);
    }
    codesysArgs.push(`--runscript="${watcherPath}"`);
    const codesysDir = path.dirname(this.config.codesysPath);

    launcherLog.info(`Spawning: ${this.config.codesysPath} ${codesysArgs.join(' ')}`);

    // Spawn CODESYS detached with UI visible
    this.process = spawn(this.config.codesysPath, codesysArgs, {
      detached: true,
      shell: false,
      windowsHide: false,
      stdio: 'ignore',
      cwd: codesysDir,
      // Pass the args exactly as written. Node's own quoting turns embedded quotes into
      // \" escapes, which CODESYS mis-parses for values with spaces. Verbatim gives the
      // same command line the CODESYS Installer uses.
      windowsVerbatimArguments: true,
    });

    this.pid = this.process.pid ?? null;
    this.process.unref();

    launcherLog.info(`CODESYS spawned with PID ${this.pid}`);

    // Handle process exit
    const spawnedPid = this.pid;
    this.process.on('exit', (code) => {
      launcherLog.warn(`CODESYS process ${spawnedPid} exited with code ${code}`);
      if (this.pid !== spawnedPid) return; // since attached elsewhere
      if (this.state !== 'stopping') {
        this.lastError = `CODESYS exited unexpectedly (code ${code})`;
        this.setState('error');
      }
      this.pid = null;
      this.process = null;
    });

    // Poll for ready.signal
    const readyStart = Date.now();
    while (Date.now() - readyStart < READY_TIMEOUT_MS) {
      if (await this.ipcClient.isReady()) {
        this.setState('ready');
        this.startedAt = Date.now();
        this.writeLease();
        launcherLog.info('CODESYS watcher is ready');
        this.startHealthMonitor();
        return { action: 'spawned', pid: this.pid, sessionId: this.sessionId, others: othersThan(this.sessionId) };
      }
      await this.sleep(READY_POLL_MS);
    }

    // Timeout — watcher never signaled ready
    this.lastError = `Watcher did not signal ready within ${READY_TIMEOUT_MS}ms`;
    this.setState('error');
    throw new Error(this.lastError);
  }

  /** Graceful shutdown */
  async shutdown(): Promise<void> {
    if (this.state === 'stopped' || this.state === 'stopping') return;

    this.setState('stopping');
    this.stopHealthMonitor();

    // Try to close projects and quit CODESYS gracefully via script
    if (this.ipcClient && this.state !== 'error') {
      try {
        launcherLog.info('Sending quit script to close projects and exit CODESYS...');
        await this.ipcClient.sendCommand(`
import sys
try:
    import scriptengine as se
    # Close all open projects without saving (save should be done before shutdown)
    for p in list(se.projects):
        try:
            p.close()
        except:
            pass
    print("Projects closed")
except:
    pass
# Request CODESYS to quit
try:
    import scriptengine as se
    se.system.exit()
except:
    pass
print("SCRIPT_SUCCESS")
sys.exit(0)
`, 10_000);
      } catch {
        launcherLog.debug('Quit script timed out or failed (expected if CODESYS exits)');
      }
    }

    // Send terminate signal to watcher
    if (this.ipcClient) {
      try {
        await this.ipcClient.sendTerminate();
      } catch {
        launcherLog.warn('Failed to send terminate signal');
      }
    }

    // Wait for process exit
    if (this.pid !== null) {
      const waitStart = Date.now();
      while (Date.now() - waitStart < SHUTDOWN_WAIT_MS) {
        if (!this.isRunning()) break;
        await this.sleep(500);
      }

      // Force kill if still alive. Never kill a PID we cannot confirm is
      // CODESYS - an adopted session may hold a recycled PID now owned by an
      // unrelated program.
      if (this.isRunning() && this.pid !== null && !this.isCodesysProcess(this.pid)) {
        launcherLog.warn(
          `Refusing to kill PID ${this.pid}: it is not a CODESYS process (stale/recycled PID). Clearing state without killing.`
        );
        this.pid = null;
        this.process = null;
      } else if (this.isRunning() && this.pid !== null) {
        launcherLog.warn('Force-killing CODESYS process');
        try {
          // On Windows, use taskkill for reliable process termination
          if (process.platform === 'win32') {
            const { execSync } = require('child_process');
            try {
              // First try graceful close (WM_CLOSE)
              execSync(`taskkill /PID ${this.pid}`, { timeout: 5000, stdio: 'ignore' });
              await this.sleep(3_000);
            } catch { /* ignore */ }
            if (this.isRunning()) {
              // Force kill
              try {
                execSync(`taskkill /F /PID ${this.pid}`, { timeout: 5000, stdio: 'ignore' });
              } catch { /* ignore */ }
            }
          } else if (this.process) {
            this.process.kill('SIGTERM');
            await this.sleep(2_000);
            if (this.isRunning() && this.process) {
              this.process.kill('SIGKILL');
            }
          }
        } catch {
          launcherLog.warn('Failed to kill CODESYS process');
        }
      }
    }

    // Clean up IPC directory
    this.stopLease();
    if (this.ipcClient) {
      await this.ipcClient.cleanup();
    }

    this.pid = null;
    this.process = null;
    this.ipcClient = null;
    this.setState('stopped');
    launcherLog.info('Shutdown complete');
  }

  /** Execute a script through the IPC channel */
  async executeScript(content: string, timeoutMs?: number): Promise<IpcResult> {
    if (this.state !== 'ready' || !this.ipcClient) {
      throw new Error(`Cannot execute script: launcher state is '${this.state}'`);
    }
    return this.ipcClient.sendCommand(content, timeoutMs);
  }

  /** Get current launcher status */
  getStatus(): LauncherStatus {
    return {
      state: this.state,
      pid: this.pid,
      sessionId: this.sessionId,
      ipcDir: this.ipcDir,
      startedAt: this.startedAt,
      lastError: this.lastError,
    };
  }

  /**
   * Every CODESYS on this PC with the session (if any) that serves it and its window
   * title, so a caller can see other sessions' instances before acting.
   */
  listInstances(): Array<{ pid: number; title: string; session: SessionInfo | null }> {
    const { sessions, snapshot } = this.scan();
    const live = sessions.filter((s) => s.codesysAlive && s.pid !== null);
    const pids = new Set<number>([...(snapshot?.keys() ?? []), ...live.map((s) => s.pid as number)]);
    return [...pids].sort((a, b) => a - b).map((pid) => ({
      pid,
      title: snapshot?.get(pid)?.title ?? '',
      session: live.find((s) => s.pid === pid) ?? null,
    }));
  }

  /** This server's owner tag, for status output. */
  ownerTag(): string | null {
    return currentOwnerTag();
  }

  /** Check if the CODESYS process is still alive */
  isRunning(): boolean {
    if (this.pid === null) return false;
    return isPidAlive(this.pid);
  }

  /** Register callback for state changes */
  onStateChange(callback: (state: CodesysState) => void): void {
    this.stateChangeCallbacks.push(callback);
  }

  private setState(state: CodesysState): void {
    const prev = this.state;
    this.state = state;
    if (prev !== state) {
      launcherLog.info(`State: ${prev} -> ${state}`);
      for (const cb of this.stateChangeCallbacks) {
        try { cb(state); } catch { /* ignore callback errors */ }
      }
    }
  }

  private startHealthMonitor(): void {
    this.stopHealthMonitor();
    this.healthInterval = setInterval(() => {
      if (this.state === 'ready' && !this.isRunning()) {
        launcherLog.error('Health check: CODESYS process died');
        this.lastError = 'CODESYS process died unexpectedly';
        this.pid = null;
        this.process = null;
        this.setState('error');
        this.stopHealthMonitor();
      }
    }, HEALTH_CHECK_INTERVAL_MS);
  }

  private stopHealthMonitor(): void {
    if (this.healthInterval) {
      clearInterval(this.healthInterval);
      this.healthInterval = null;
    }
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}
