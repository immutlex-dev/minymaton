// Builds the signed release APK (Hermes, arm64-v8a + x86_64) and proves it:
// apksigner verifies the signature, the archive carries both ABIs' Rust core
// and Hermes runtime plus the compiled JS bundle, and the SHA-256 is printed.
// Signing comes from MINYMATON_RELEASE_* (environment or Gradle property)
// pointing at a keystore kept outside Git.
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const { spawnSync } = require('child_process');
const { ANDROID, gradle, prepareAndroid, run } = require('./android_env');

const ABIS = ['arm64-v8a', 'x86_64'];
const BUILD_TOOLS = '35.0.0';

function fail(message) {
  throw new Error(message);
}

for (const name of [
  'MINYMATON_RELEASE_STORE_FILE',
  'MINYMATON_RELEASE_STORE_PASSWORD',
  'MINYMATON_RELEASE_KEY_ALIAS',
  'MINYMATON_RELEASE_KEY_PASSWORD',
]) {
  if (!process.env[name]) fail(`release signing is not configured: ${name} is unset`);
}

prepareAndroid();
run('python', ['tools/build_mobile_core.py', ...ABIS.flatMap((abi) => ['--abi', abi])], { fromRepo: true });
gradle(['assembleRelease']);

const apk = path.join(ANDROID, 'app', 'build', 'outputs', 'apk', 'release', 'app-release.apk');
if (!fs.existsSync(apk)) fail(`release APK missing: ${apk}`);

const apksigner = path.join(
  process.env.ANDROID_HOME,
  'build-tools',
  BUILD_TOOLS,
  process.platform === 'win32' ? 'apksigner.bat' : 'apksigner',
);
const signed = spawnSync(apksigner, ['verify', '--print-certs', apk], {
  encoding: 'utf8',
  shell: process.platform === 'win32',
});
if (signed.status !== 0) fail(`apksigner rejected ${apk}:\n${signed.stdout}${signed.stderr}`);
if (/CN=Android Debug/.test(signed.stdout)) fail('release APK is signed with the debug key');
process.stdout.write(signed.stdout);

const listing = spawnSync(
  'python',
  ['-c', 'import sys, zipfile; print("\\n".join(zipfile.ZipFile(sys.argv[1]).namelist()))', apk],
  { encoding: 'utf8' },
);
if (listing.status !== 0) fail(`cannot list ${apk}: ${listing.stderr}`);
const entries = new Set(listing.stdout.split(/\r?\n/));
const required = ['assets/index.android.bundle'];
for (const abi of ABIS) {
  required.push(`lib/${abi}/libminymaton_mobile_core.so`, `lib/${abi}/libhermes.so`);
}
const missing = required.filter((entry) => !entries.has(entry));
if (missing.length) fail(`release APK is missing ${missing.join(', ')}`);
const extraAbis = [...entries]
  .map((entry) => /^lib\/([^/]+)\//.exec(entry)?.[1])
  .filter((abi) => abi && !ABIS.includes(abi));
if (extraAbis.length) fail(`release APK carries unexpected ABIs: ${[...new Set(extraAbis)].join(', ')}`);

const sha256 = crypto.createHash('sha256').update(fs.readFileSync(apk)).digest('hex');
console.log(JSON.stringify({ apk, bytes: fs.statSync(apk).size, sha256, abis: ABIS }));
