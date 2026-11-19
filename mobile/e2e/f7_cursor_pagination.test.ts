// F7: cursor-based pagination for channels (`STREAMS`) and messages (`HISTORY`),
// stepping forward/backward via opaque `next_after` and `before` cursors.
import { by, device, element, expect } from 'detox';
import { Bus, PHONE_TOKEN } from './bus';
import { connect, expectGone, expectText, reveal, tap } from './ui';

describe('F7 cursor pagination for channels and messages', () => {
  let bus: Bus;

  beforeAll(async () => {
    bus = await Bus.start(device.id);
    await device.launchApp({ delete: true, newInstance: true });
  });

  afterAll(async () => {
    await bus.stop();
  });

  it('paginates channels via next_after and messages via before cursors', async () => {
    const desk = bus.desk();
    for (let idx = 1; idx <= 24; idx += 1) {
      await desk.createStream(`chan-${String(idx).padStart(2, '0')}`);
    }
    await desk.join('chan-24');
    for (let n = 1; n <= 25; n += 1) {
      await desk.publish('chan-24', { text: `entry ${n}` });
    }

    await connect('Home', bus.url, PHONE_TOKEN);

    await expectText('streams_page_info', 'PAGE 1+ (20)');
    await expect(element(by.id('stream_agents'))).toExist();
    await expect(element(by.id('stream_chan-01'))).toExist();
    await expectGone('stream_chan-24');

    await tap('streams_next');
    await expectText('streams_page_info', 'PAGE 2 (5)');
    await expect(element(by.id('stream_chan-20'))).toExist();
    await expect(element(by.id('stream_chan-24'))).toExist();
    await expectGone('stream_chan-01');

    await tap('streams_prev');
    await expectText('streams_page_info', 'PAGE 1+ (20)');
    await expect(element(by.id('stream_chan-01'))).toExist();

    await tap('streams_next');
    await tap('stream_chan-24');
    await expectText('open_stream', 'chan-24');

    await expectText('messages_page_info', 'Page 1 (20/25)');
    await expectText('messages_cursor_range', '#0025..#0006');
    await expectText('message_25', 'entry 25');
    await reveal('message_6');
    await expectText('message_6', 'entry 6');
    await expectGone('message_5');
    await expectGone('message_1');

    await tap('messages_older');
    await expectText('messages_page_info', 'Page 2 (5/25)');
    await expectText('messages_cursor_range', '#0005..#0001');
    await expectText('message_5', 'entry 5');
    await reveal('message_1');
    await expectText('message_1', 'entry 1');
    await expectGone('message_25');

    await tap('messages_newer');
    await expectText('messages_page_info', 'Page 1 (20/25)');
    await expectText('message_25', 'entry 25');
    await expectGone('message_1');
  });
});
