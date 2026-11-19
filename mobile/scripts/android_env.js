// Shared Android build environment for the build scripts: the SDK, the JDK,
// local.properties, and process helpers that fail loudly.
const fs = require('fs');
const os = require('os');
const path = require('path');
const { execFileSync, spawnSync } = require('child_process');

const MOBILE = path.resolve(__dirname, '..');
const REPO = path.resolve(MOBILE, '..');
const ANDROID = path.join(MOBILE, 'android');

function resolveAndroidSdk() {
  const home = os.homedir();
  return [
    process.env.ANDROID_HOME,
    process.env.ANDROID_SDK_ROOT,
    home && path.join(home, 'Android', 'Sdk'),
    home && path.join(home, 'Library', 'Android', 'sdk'),
    process.env.LOCALAPPDATA && path.join(process.env.LOCALAPPDATA, 'Android', 'Sdk'),
  ]
    .filter(Boolean)
    .find((candidate) => fs.existsSync(candidate));
}

function resolveJavaHome() {
  if (process.env.JAVA_HOME) return process.env.JAVA_HOME;
  const finder = process.platform === 'win32' ? 'where' : 'which';
  const javac = execFileSync(finder, ['javac'], { encoding: 'utf8' }).split(/\r?\n/).find(Boolean);
  return javac ? path.dirname(path.dirname(fs.realpathSync(javac))) : null;
}

function prepareAndroid() {
  const sdk = resolveAndroidSdk();
  if (!sdk) throw new Error('no Android SDK: set ANDROID_HOME');
  process.env.ANDROID_HOME = sdk;
  process.env.ANDROID_SDK_ROOT = sdk;
  const java = resolveJavaHome();
  if (!java) throw new Error('no JDK: set JAVA_HOME');
  process.env.JAVA_HOME = java;
  const local = path.join(ANDROID, 'local.properties');
  if (!fs.existsSync(local)) {
    fs.writeFileSync(local, `sdk.dir=${sdk.replace(/\\/g, '/')}\n`);
  }
}

function run(command, args, { fromRepo = false, env = {} } = {}) {
  const result = spawnSync(command, args, {
    cwd: fromRepo ? REPO : MOBILE,
    stdio: 'inherit',
    env: { ...process.env, ...env },
  });
  if (result.error) throw result.error;
  if (result.status !== 0) {
    throw new Error(`${command} ${args.join(' ')} exited with ${result.status}`);
  }
}

function gradle(args, env = {}) {
  // The repository carries only the POSIX wrapper (no .bat scripts).
  const result = spawnSync('sh', ['./gradlew', ...args], {
    cwd: ANDROID,
    stdio: 'inherit',
    env: { ...process.env, ...env },
  });
  if (result.error) throw result.error;
  if (result.status !== 0) throw new Error(`gradle ${args.join(' ')} exited with ${result.status}`);
}

module.exports = { ANDROID, MOBILE, REPO, gradle, prepareAndroid, run };
