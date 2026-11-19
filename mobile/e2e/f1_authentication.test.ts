// F1: bearer authentication. A wrong token is refused by the real bus and
// leaves no profile behind; the right token activates it, and the token is
// held only as platform keystore ciphertext (Android Keystore / iOS
// Keychain), never in the core's database.
import { expect as jestExpect } from '@jest/globals';
import { by, device, element, expect } from 'detox';
import { Bus, PHONE_TOKEN, listAppFiles, readAppFile } from './bus';
import { connect, expectGone, expectText } from './ui';

describe('F1 authentication', () => {
  let bus: Bus;

  beforeAll(async () => {
    bus = await Bus.start(device.id);
    await device.launchApp({ delete: true, newInstance: true });
  });

  afterAll(async () => {
    await bus.stop();
  });

  it('refuses a wrong token and keeps no profile', async () => {
    await connect('Home', bus.url, 'mny_wrong_00000000000000000000000000000000');
    await expectText('active_bus', 'no bus');
    await expect(element(by.id('error_text'))).toExist();
    await expectGone('profile_Home');
  });

  it('activates with the enrolled token and seals it in the platform keystore', async () => {
    await connect('Home', bus.url, PHONE_TOKEN);
    await expectText('active_bus', 'Home');
    await expect(element(by.id('profile_Home'))).toExist();

    const token = Buffer.from(PHONE_TOKEN);
    const coreFiles = listAppFiles(device.id, 'files/minymaton');
    jestExpect(coreFiles).toContain('minymaton-mobile.sqlite3');
    for (const name of coreFiles) {
      const bytes = readAppFile(device.id, `files/minymaton/${name}`);
      jestExpect(bytes.includes(token)).toBe(false);
    }
    if (device.getPlatform() === 'android') {
      const sealed = readAppFile(device.id, 'shared_prefs/minymaton_bus_tokens.xml').toString();
      jestExpect(sealed.includes(PHONE_TOKEN)).toBe(false);
      jestExpect(sealed).toMatch(/<string name="[0-9a-f]+">[A-Za-z0-9+/=]+:[A-Za-z0-9+/=]+<\/string>/);
    } else {
      // iOS: the token lives only in the Keychain, which is not a file in the
      // app container. No container file may carry it either.
      for (const name of listAppFiles(device.id, 'Library')) {
        jestExpect(readAppFile(device.id, `Library/${name}`).includes(token)).toBe(false);
      }
    }
  });
});
