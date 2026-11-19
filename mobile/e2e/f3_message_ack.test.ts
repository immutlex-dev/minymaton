// F3: a direct message wakes the phone's parked wait; the phone shows it,
// the bus commits the acknowledgement and the phone parks again. A nudge
// from the phone reaches the desk with the shared wire payload.
import { expect as jestExpect } from '@jest/globals';
import { device } from 'detox';
import { Bus, PHONE_TOKEN } from './bus';
import { connect, expectText, fill, join, openStream, tap } from './ui';

describe('F3 message, ACK and wakeup', () => {
  let bus: Bus;

  beforeAll(async () => {
    bus = await Bus.start(device.id);
    await device.launchApp({ delete: true, newInstance: true });
  });

  afterAll(async () => {
    await bus.stop();
  });

  it('wakes on a direct message, acknowledges it and re-parks', async () => {
    const desk = bus.desk();
    await desk.createStream('ops');
    const deskHandle = await desk.join('ops');
    await connect('Home', bus.url, PHONE_TOKEN);
    await openStream('ops');
    await join('phone');

    const sequence = await desk.publish('ops', { text: 'deploy done?' }, 'phone');
    await expectText(`message_${sequence}`, 'deploy done?');
    await expectText(`message_from_${sequence}`, 'desk');
    await expectText('ack_status', 'committed');
    // The delivery set watch_status to "handling ops", so reading "waiting"
    // again means the core parked a new wait after the acknowledgement.
    await expectText('watch_status', 'waiting ops as phone');
    const roster = await desk.roster('ops');
    jestExpect(roster.find((entry) => entry.label === 'phone')).toMatchObject({ label: 'phone', waiting: true });

    const arrival = desk.waitForEvent(deskHandle);
    await tap('nudge_yes_go');
    const events = await arrival;
    jestExpect(events).toHaveLength(1);
    jestExpect(events[0].payload).toEqual({
      from: 'phone',
      nudge: 'yes_go',
      text: 'yes / go',
      source: 'minymaton-mobile',
    });

    const bystanderHandle = await desk.join('ops', 'bystander');
    await openStream('ops');
    await tap('target_desk');
    await expectText('target_pill', 'TARGET: @desk');

    const directArrival = desk.waitForEvent(deskHandle);
    await fill('compose_text', 'ship it directly');
    await tap('send_button');
    const directEvents = await directArrival;
    jestExpect(directEvents).toHaveLength(1);
    jestExpect(directEvents[0].payload).toEqual({
      from: 'phone',
      to: 'desk',
      text: 'ship it directly',
    });
    await expectText(`message_route_${directEvents[0].sequence}`, 'phone -> @desk');
    await desk.checkEmpty(bystanderHandle);
  });
});
