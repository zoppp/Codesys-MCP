import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import {
  classifyOwnership,
  scanSessionsIn,
  pickImplicitAdoption,
  selectStaleSessions,
  SessionOwner,
} from '../../src/launcher';

const NOW = 1_000_000_000;
const ME = { tag: 'local_me', serverId: 'srv-me', serverPid: 100, parentPid: 10 };
const alive = (pids: number[]) => (pid: number) => pids.includes(pid);

function owner(o: Partial<SessionOwner>): SessionOwner {
  return { tag: 'local_other', serverId: 'srv-other', serverPid: 200, parentPid: 20, heartbeat: NOW, ...o };
}

describe('classifyOwnership', () => {
  it('no lease is unowned', () => {
    expect(classifyOwnership(null, ME, NOW).ownership).toBe('unowned');
  });

  it('own server id is self', () => {
    expect(classifyOwnership(owner({ serverId: 'srv-me', serverPid: 100 }), ME, NOW, alive([100])).ownership).toBe('self');
  });

  it('a recycled server pid of another session is not self', () => {
    expect(classifyOwnership(owner({ serverPid: 100 }), ME, NOW, alive([100])).ownership).toBe('other-live');
  });

  it('same tag is same-session even while the old server lives', () => {
    expect(classifyOwnership(owner({ tag: 'local_me' }), ME, NOW, alive([200])).ownership).toBe('same-session');
  });

  it('fresh lease of a live server of another session is other-live', () => {
    const r = classifyOwnership(owner({ heartbeat: NOW - 5_000 }), ME, NOW, alive([200]));
    expect(r).toEqual({ ownership: 'other-live', leaseFresh: true });
  });

  it('stale lease is orphan', () => {
    expect(classifyOwnership(owner({ heartbeat: NOW - 60_000 }), ME, NOW, alive([200])).ownership).toBe('orphan');
  });

  it('fresh lease whose server died is orphan', () => {
    expect(classifyOwnership(owner({}), ME, NOW, alive([])).ownership).toBe('orphan');
  });
});

describe('scanSessionsIn + pickImplicitAdoption', () => {
  let root: string;

  beforeEach(() => {
    root = fs.mkdtempSync(path.join(os.tmpdir(), 'cmcp-scan-'));
  });

  afterEach(() => {
    fs.rmSync(root, { recursive: true, force: true });
  });

  function session(id: string, codesysPid: number | null, lease: SessionOwner | null, readyAgeMs = 0) {
    const dir = path.join(root, id);
    fs.mkdirSync(dir);
    if (codesysPid !== null) {
      const sig = path.join(dir, 'ready.signal');
      fs.writeFileSync(sig, JSON.stringify({ pid: codesysPid, python_version: 'CODESYS V3.5 SP19 Patch 7' }));
      const t = (Date.now() - readyAgeMs) / 1000;
      fs.utimesSync(sig, t, t);
    }
    if (lease) fs.writeFileSync(path.join(dir, 'owner.json'), JSON.stringify(lease));
  }

  const scan = (codesys: number[], servers: number[]) =>
    scanSessionsIn(root, ME, alive(codesys), NOW, alive(servers));

  it("never takes another live session's instance (the old newest-signal hijack)", () => {
    session('mine-old', 1, owner({ tag: 'local_me', heartbeat: NOW - 600_000 }), 60_000);
    session('theirs-newest', 2, owner({ heartbeat: NOW }), 0);
    const pick = pickImplicitAdoption(scan([1, 2], [200]), ME);
    expect(pick?.id).toBe('mine-old');
  });

  it('spawns fresh (null) when only other sessions, orphans or unowned instances are live', () => {
    session('theirs', 2, owner({}));
    session('orphan', 3, owner({ heartbeat: NOW - 600_000 }));
    session('handmade', 4, null);
    expect(pickImplicitAdoption(scan([2, 3, 4], [200]), ME)).toBeNull();
  });

  it('skips a session whose CODESYS is dead or recycled', () => {
    session('mine-dead', 5, owner({ tag: 'local_me' }));
    expect(pickImplicitAdoption(scan([], []), ME)).toBeNull();
  });

  it("leaves this session's instance alone while another live server of it drives it", () => {
    session('child-driven', 12, owner({ tag: 'local_me', serverId: 'srv-child', serverPid: 300 }));
    expect(pickImplicitAdoption(scan([12], [300]), ME)).toBeNull();
  });

  it('prefers the newest of several own instances', () => {
    session('a', 6, owner({ tag: 'local_me' }), 30_000);
    session('b', 7, owner({ tag: 'local_me' }), 1_000);
    expect(pickImplicitAdoption(scan([6, 7], []), ME)?.id).toBe('b');
  });

  it('without any session identity, only an orphan of the same parent process', () => {
    const anon = { tag: null, serverId: 'srv-me', serverPid: 100, parentPid: 10 };
    session('same-parent', 8, owner({ tag: null, parentPid: 10, heartbeat: NOW - 600_000 }));
    session('other-parent', 9, owner({ tag: null, parentPid: 99, heartbeat: NOW - 600_000 }));
    const sessions = scanSessionsIn(root, anon, alive([8, 9]), NOW, alive([]));
    expect(pickImplicitAdoption(sessions, anon)?.id).toBe('same-parent');
  });

  it('reports pid, lease and liveness per dir and ignores non-directories', () => {
    session('x', 11, owner({}));
    fs.writeFileSync(path.join(root, 'stray.txt'), '');
    const [s] = scan([11], [200]);
    expect(s).toMatchObject({ id: 'x', pid: 11, codesysAlive: true, ownership: 'other-live', leaseFresh: true });
    expect(scan([11], [200])).toHaveLength(1);
  });

  it('passes the ready.signal mtime to the CODESYS probe (recycled-PID check)', () => {
    session('r', 13, null, 5_000);
    const seen: Array<number | null> = [];
    scanSessionsIn(root, ME, (_pid, ready) => { seen.push(ready); return true; }, NOW, alive([]));
    expect(seen[0]).toBeGreaterThan(0);
  });
});

describe('selectStaleSessions', () => {
  const HOUR = 60 * 60_000;
  const base = {
    dir: 'x', pid: 1, pythonVersion: null, readyMtime: null, owner: null,
    ownership: 'unowned' as const, leaseFresh: false,
  };

  it('deletes only dead, unleased, idle dirs', () => {
    const dead = { ...base, id: 'dead', codesysAlive: false, newestMtime: NOW - 2 * HOUR };
    const live = { ...base, id: 'live', codesysAlive: true, newestMtime: NOW - 5 * HOUR };
    const leased = { ...base, id: 'leased', codesysAlive: false, leaseFresh: true, newestMtime: NOW - 5 * HOUR };
    const young = { ...base, id: 'young', codesysAlive: false, newestMtime: NOW - 10 * 60_000 };
    const mine = { ...base, id: 'mine', codesysAlive: false, ownership: 'self' as const, newestMtime: NOW - 5 * HOUR };
    const ids = selectStaleSessions([dead, live, leased, young, mine], NOW).map((s) => s.id);
    expect(ids).toEqual(['dead']);
  });
});
