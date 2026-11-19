// Core events as the device logs them (MinymatonModule.emit logs each event's
// kind under the MinymatonCore tag). While the app is in the background the
// screen cannot be read, so a flow that needs to know the core reached a
// state then reads this stream instead: a logcat process the host follows
// line by line, without polling.
import { ChildProcess, execFileSync, spawn } from 'child_process';
import readline from 'readline';

function isAndroid(): boolean {
  try {
    const { device } = require('detox');
    return device.getPlatform() === 'android';
  } catch {
    return true;
  }
}

export class CoreLog {
  private readonly seen: string[] = [];
  private readonly wanted = new Map<string, () => void>();

  private constructor(
    private readonly child: ChildProcess,
    pattern: RegExp,
  ) {
    const lines = readline.createInterface({ input: child.stdout! });
    lines.on('line', (line) => {
      const match = pattern.exec(line.trim());
      if (!match) return;
      this.seen.push(match[1]);
      this.wanted.get(match[1])?.();
    });
  }

  /** Follows the core's events from now on (earlier log lines are cleared). */
  static follow(serial: string): CoreLog {
    if (!isAndroid()) {
      // MinymatonModule NSLogs each event kind as 'MinymatonCore: <kind>'.
      return new CoreLog(
        spawn(
          'xcrun',
          ['simctl', 'spawn', serial, 'log', 'stream', '--level', 'info', '--style', 'compact', '--predicate', 'eventMessage CONTAINS "MinymatonCore:"'],
          { stdio: ['ignore', 'pipe', 'inherit'] },
        ),
        /MinymatonCore: (\S+)$/,
      );
    }
    execFileSync('adb', ['-s', serial, 'logcat', '-c']);
    return new CoreLog(spawn('adb', ['-s', serial, 'logcat', '-v', 'raw', '-s', 'MinymatonCore:I'], { stdio: ['ignore', 'pipe', 'inherit'] }), /^event (\S+)$/);
  }

  /** Resolves when the core has emitted `kind`, or at once if it already has. */
  next(kind: string): Promise<void> {
    if (this.seen.includes(kind)) return Promise.resolve();
    return new Promise((resolve, reject) => {
      this.wanted.set(kind, resolve);
      this.child.once('exit', (code) => reject(new Error(`logcat exited with ${code} before core event ${kind}`)));
    });
  }

  async stop(): Promise<void> {
    if (this.child.exitCode !== null) return;
    const exited = new Promise((resolve) => this.child.once('exit', resolve));
    this.child.kill();
    await exited;
  }
}
