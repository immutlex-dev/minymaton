// F6: top-docked DISPATCH DECK (zero scrolling needed) + state-driven
// sweep_idle and multi-select bulk delete for idle agents and streams.
import { expect as jestExpect } from '@jest/globals';
import { by, device, element, expect } from 'detox';
import { Bus, PHONE_TOKEN } from './bus';
import { connect, expectGone, expectText, fill, join, openStream, tap } from './ui';

describe('F6 top dispatch deck and sweep UI', () => {
  let bus: Bus;

  beforeAll(async () => {
    bus = await Bus.start(device.id);
    await device.launchApp({ delete: true, newInstance: true });
  });

  afterAll(async () => {
    await bus.stop();
  });

  it('dispatches from the top deck with zero scroll and sweeps/bulk-deletes idle agents and streams', async () => {
    const desk = bus.desk();
    await desk.createStream('stale-chan');
    await desk.join('stale-chan', 'stale-sub');

    await desk.createStream('ops');
    await desk.join('ops', 'dead-1');
    await desk.join('ops', 'dead-2');
    const deskHandle = await desk.join('ops', 'desk');

    // Park desk so `desk` is waiting:true before the phone opens `ops`.
    const deskWait1 = await desk.park(deskHandle);

    await connect('Home', bus.url, PHONE_TOKEN);
    await openStream('ops');

    // Top-docked DISPATCH DECK must be immediately visible with ZERO scrolling!
    await expect(element(by.id('compose_text'))).toBeVisible();
    await expect(element(by.id('send_button'))).toBeVisible();

    await join('phone');
    await expect(element(by.id('compose_text'))).toBeVisible();
    await expect(element(by.id('send_button'))).toBeVisible();

    // Send a direct message to @desk right from the top deck.
    await tap('target_desk');
    await expectText('target_pill', 'TARGET: @desk');
    await fill('compose_text', 'direct from top deck');
    await tap('send_button');
    const directEvents = await deskWait1.done;
    jestExpect(directEvents).toHaveLength(1);
    jestExpect(directEvents[0].payload).toEqual({
      from: 'phone',
      to: 'desk',
      text: 'direct from top deck',
    });

    // Re-park desk so sweep_roster protects `desk` and `phone` while sweeping `dead-1` & `dead-2`.
    const deskWait2 = await desk.park(deskHandle);
    await tap('sweep_roster');
    await expectGone('roster_dead-1');
    await expectGone('roster_dead-2');
    await expectText('roster_desk', 'desk waiting');
    await expectText('roster_phone', 'phone waiting');

    // Create two more idle subs (`trim-a`, `trim-b`), reload stream, and bulk-delete them.
    await desk.join('ops', 'trim-a');
    await desk.join('ops', 'trim-b');
    await openStream('ops');
    await expectText('roster_trim-a', 'trim-a idle');
    await expectText('roster_trim-b', 'trim-b idle');

    await tap('toggle_select_roster');
    await tap('select_roster_trim-a');
    await tap('select_roster_trim-b');
    await tap('delete_selected_roster');
    await expectGone('roster_trim-a');
    await expectGone('roster_trim-b');
    await expectText('roster_desk', 'desk waiting');
    await expectText('roster_phone', 'phone waiting');

    // Sweep idle streams: `stale-chan` vanishes while `agents` and active `ops` remain.
    await tap('sweep_streams');
    await expectGone('stream_stale-chan');
    await expect(element(by.id('stream_agents'))).toExist();
    await expect(element(by.id('stream_ops'))).toExist();

    // Create an idle stream `drop-me`, select it, and bulk-delete it.
    await desk.createStream('drop-me');
    await tap('refresh_streams');
    await tap('toggle_select_streams');
    await tap('select_stream_drop-me');
    await tap('delete_selected_streams');
    await expectGone('stream_drop-me');

    // Wake and drain `deskWait2` cleanly from a distinct sender before stopping the bus.
    await desk.publish('ops', { text: 'cleanup' }, 'desk', 'cleanup');
    await deskWait2.done;
  });
});
