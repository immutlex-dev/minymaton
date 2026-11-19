// F5: switching buses. Each bus keeps its own streams and membership; a
// message published on the inactive bus arrives once the phone switches
// back; and a cold start reactivates the last bus with its Keystore token.
import { by, device, element, expect } from 'detox';
import { Bus, PHONE_TOKEN } from './bus';
import { connect, expectGone, expectText, join, openStream, tap } from './ui';

describe('F5 switching buses', () => {
  let home: Bus;
  let work: Bus;

  beforeAll(async () => {
    home = await Bus.start(device.id);
    work = await Bus.start(device.id);
    await device.launchApp({ delete: true, newInstance: true });
  });

  afterAll(async () => {
    await home.stop();
    await work.stop();
  });

  it('switches between buses and restores the active one on a cold start', async () => {
    const homeDesk = home.desk();
    const workDesk = work.desk();
    await homeDesk.createStream('alpha');
    await workDesk.createStream('beta');

    await connect('Home', home.url, PHONE_TOKEN);
    await expectText('active_bus', 'Home');
    await openStream('alpha');
    await join('phone');

    await tap('show_add_bus');
    await connect('Work', work.url, PHONE_TOKEN);
    await expectText('active_bus', 'Work');
    await openStream('beta');
    await expect(element(by.id('stream_alpha'))).not.toExist();

    const sequence = await homeDesk.publish('alpha', { text: 'while you were on work' }, 'phone');

    await tap('profile_Home');
    await expectText('active_bus', 'Home');
    await expectGone('stream_beta');
    await openStream('alpha');
    await expectText(`message_${sequence}`, 'while you were on work');
    await expectText('ack_status', 'committed');

    await device.launchApp({ newInstance: true });
    await expectText('active_bus', 'Home');
    await openStream('alpha');
    await expectText(`message_${sequence}`, 'while you were on work');
    await expectText('roster_phone', 'phone waiting');
  });
});
