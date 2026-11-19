// F4: background catch-up. Backgrounding pauses the core, which returns only
// once its wait on the bus has ended; messages published meanwhile are
// delivered in order and acknowledged when the app returns to the foreground.
import { expect as jestExpect } from '@jest/globals';
import { device } from 'detox';
import { Bus, PHONE_TOKEN } from './bus';
import { CoreLog } from './core_log';
import { connect, expectText, join, openStream } from './ui';

describe('F4 background catch-up', () => {
  let bus: Bus;

  beforeAll(async () => {
    bus = await Bus.start(device.id);
    await device.launchApp({ delete: true, newInstance: true });
  });

  afterAll(async () => {
    await bus.stop();
  });

  it('catches up on everything published while in the background', async () => {
    const desk = bus.desk();
    await desk.createStream('ops');
    await desk.join('ops');
    await connect('Home', bus.url, PHONE_TOKEN);
    await openStream('ops');
    await join('phone');

    // The bus learns of a closed connection on its own schedule, so its
    // roster is not read here; the core's awaited pause is the signal.
    const log = CoreLog.follow(device.id);
    try {
      await device.sendToHome();
      await log.next('paused');
    } finally {
      await log.stop();
    }
    const texts = ['first', 'second', 'third'];
    const sequences: number[] = [];
    for (const text of texts) {
      sequences.push(await desk.publish('ops', { text }));
    }

    await device.launchApp({ newInstance: false });
    await expectText('pause_status', 'stopped ops');
    for (const [index, sequence] of sequences.entries()) {
      await expectText(`message_${sequence}`, texts[index]);
    }
    await expectText('ack_status', 'committed');
    await expectText('watch_status', 'waiting ops as phone');
    const roster = await desk.roster('ops');
    jestExpect(roster.find((entry) => entry.label === 'phone')).toMatchObject({ label: 'phone', waiting: true });
  });
});
