// The debug APK bundles its JS
// (DETOX_BUNDLE_IN_DEBUG), so no Metro server or port reversal is needed;
// each bus port is reversed by e2e/bus.ts when its server starts.
/** @type {Detox.DetoxConfig} */
module.exports = {
  testRunner: {
    args: {
      $0: 'jest',
      config: 'e2e/jest.config.js',
    },
    jest: {
      setupTimeout: 240000,
    },
  },
  apps: {
    'ios.debug': {
      type: 'ios.app',
      binaryPath: '/tmp/minymaton-ios/Minymaton.app',
      build: 'node scripts/detox_build_ios.js',
    },
    'android.debug': {
      type: 'android.apk',
      binaryPath: 'android/app/build/outputs/apk/debug/app-debug.apk',
      testBinaryPath: 'android/app/build/outputs/apk/androidTest/debug/app-debug-androidTest.apk',
      build: 'node scripts/detox_build_android.js',
    },
  },
  devices: {
    simulator: {
      type: 'ios.simulator',
      device: {
        id: process.env.MINYMATON_IOS_UDID || '9EA279CF-1E40-4E8A-AD82-34D64DF5E5A9',
      },
    },
    attached: {
      type: 'android.attached',
      device: {
        adbName: process.env.MINYMATON_ADB_NAME || 'emulator-5554',
      },
    },
    emulator: {
      type: 'android.emulator',
      bootArgs: '-no-window -gpu swiftshader_indirect -no-snapshot -no-audio',
      device: {
        avdName: process.env.MINYMATON_AVD || 'minymaton_api34',
      },
    },
  },
  configurations: {
    'ios.sim.debug': {
      device: 'simulator',
      app: 'ios.debug',
    },
    'android.att.debug': {
      device: 'attached',
      app: 'android.debug',
    },
    'android.emu.debug': {
      device: 'emulator',
      app: 'android.debug',
    },
  },
};
