// Build the Rust core for the iOS simulator, bundle the JS, and build the app
// for Detox. Mirrors scripts/detox_build_android.js: the debug app bundles its
// JS (FORCE_BUNDLING for the Xcode bundle phase, as DETOX_BUNDLE_IN_DEBUG does
// for Gradle), so no Metro server is needed while Detox runs.
const { execFileSync } = require('node:child_process');
const fs = require('node:fs');
const path = require('node:path');

const mobileDir = __dirname.replace(/\/scripts$/, '');
const repoRoot = path.resolve(mobileDir, '..');
const iosDir = path.join(mobileDir, 'ios');

function run(command, args, options = {}) {
  execFileSync(command, args, { stdio: 'inherit', ...options });
}

// 1. Rust core static library for the simulator target.
run('cargo', ['build', '--locked', '-p', 'minymaton-mobile-core', '--target', 'aarch64-apple-ios-sim'], { cwd: repoRoot });
const libSrc = path.join(repoRoot, 'target/aarch64-apple-ios-sim/debug/libminymaton_mobile_core.a');
const libDstDir = path.join(iosDir, 'MinymatonCore/Libraries');
fs.mkdirSync(libDstDir, { recursive: true });
fs.copyFileSync(libSrc, path.join(libDstDir, 'libminymaton_mobile_core.a'));

// 2. Pods.
run('pod', ['install'], { cwd: iosDir });

// 3. App for the simulator (debug, bundles JS via the Xcode phase).
// Build only for the Detox simulator: the Rust core above is arm64-only,
// so an all-architecture build would fail to link its x86_64 slice.
const udid = process.env.MINYMATON_IOS_UDID || '9EA279CF-1E40-4E8A-AD82-34D64DF5E5A9';
run('xcodebuild', [
  '-workspace', path.join(iosDir, 'Minymaton.xcworkspace'),
  '-scheme', 'Minymaton',
  '-configuration', 'Debug',
  '-destination', `platform=iOS Simulator,id=${udid}`,
  '-sdk', 'iphonesimulator',
  '-derivedDataPath', path.join(iosDir, 'build'),
  'build',
], { env: { ...process.env, FORCE_BUNDLING: 'true' } });
// simctl cannot install a bundle from a path containing spaces (the repo
// may live on such a volume), so Detox installs a staged copy instead;
// .detoxrc.js points ios.debug.binaryPath at it.
const builtApp = path.join(iosDir, 'build/Build/Products/Debug-iphonesimulator/Minymaton.app');
const stagedApp = '/tmp/minymaton-ios/Minymaton.app';
fs.rmSync(path.dirname(stagedApp), { recursive: true, force: true });
fs.mkdirSync(path.dirname(stagedApp), { recursive: true });
fs.cpSync(builtApp, stagedApp, { recursive: true });
console.log('iOS Detox build complete.');
