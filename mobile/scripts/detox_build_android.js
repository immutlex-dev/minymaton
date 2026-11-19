// Builds everything the Detox suite runs: the Rust core for the APK's ABIs,
// the minymaton-server the tests start as real buses, and the debug and
// androidTest APKs with the JS bundled in (no Metro).
const { gradle, prepareAndroid, run } = require('./android_env');

prepareAndroid();
run('python', ['tools/build_mobile_core.py'], { fromRepo: true });
run('cargo', ['build', '--locked', '--release', '-p', 'minymaton-server'], { fromRepo: true });
gradle(['assembleDebug', 'assembleAndroidTest', '-DtestBuildType=debug'], { DETOX_BUNDLE_IN_DEBUG: 'true' });
