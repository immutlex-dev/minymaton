// F2: streams and the roster. The phone lists what the desk created, sees
// the desk on the roster, joins, and the bus reports it as waiting.
import { expect as jestExpect } from '@jest/globals';
import { by, device, element, expect } from 'detox';
import { Bus, PHONE_TOKEN } from './bus';
import { connect, expectText, join, openStream } from './ui';

describe('F2 streams and roster', () => {
  let bus: Bus;

  beforeAll(async () => {
    bus = await Bus.start(device.id);
    await device.launchApp({ delete: true, newInstance: true });
  });

  afterAll(async () => {
    await bus.stop();
  });

  it('lists streams, shows the roster and joins as a waiting member', async () => {
    const desk = bus.desk();
    await desk.createStream('ops');
    await desk.join('ops');

    await connect('Home', bus.url, PHONE_TOKEN);
    await expectText('active_bus', 'Home');
    await openStream('ops');
    await expect(element(by.id('roster_desk'))).toHaveText('desk idle');
    await expect(element(by.id('roster_phone'))).not.toExist();

    await join('phone');
    // The core reports waiting on the bus's window-open progress message,
    // which the server sends after registering the wait: one read suffices.
    await expectText('watch_status', 'waiting ops as phone');
    const roster = await desk.roster('ops');
    jestExpect(roster.find((entry) => entry.label === 'phone')).toMatchObject({ label: 'phone', waiting: true });
  });
});
