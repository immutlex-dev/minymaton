// Screen helpers shared by the flow tests. Everything lives in one scroll
// view, so an element is scrolled into view before it is touched.
import { by, device, element, expect, waitFor } from 'detox';

const SETTLE = 30000;

export async function reveal(id: string): Promise<void> {
  await element(by.id('main_scroll')).scrollTo('top');
  await waitFor(element(by.id(id)))
    .toBeVisible()
    .whileElement(by.id('main_scroll'))
    .scroll(250, 'down');
}

export async function tap(id: string): Promise<void> {
  await reveal(id);
  await element(by.id(id)).tap();
}

export async function fill(id: string, text: string): Promise<void> {
  await reveal(id);
  await element(by.id(id)).replaceText(text);
}

export async function expectText(id: string, text: string): Promise<void> {
  await waitFor(element(by.id(id))).toHaveText(text).withTimeout(SETTLE);
}

export async function expectGone(id: string): Promise<void> {
  await waitFor(element(by.id(id))).not.toExist().withTimeout(SETTLE);
}

export async function connect(name: string, url: string, token: string): Promise<void> {
  await fill('profile_name', name);
  await fill('profile_url', url);
  await fill('profile_token', token);
  await tap('add_profile');
}

export async function openStream(stream: string): Promise<void> {
  await tap('refresh_streams');
  await waitFor(element(by.id(`stream_${stream}`))).toExist().withTimeout(SETTLE);
  await tap(`stream_${stream}`);
  await expect(element(by.id('open_stream'))).toHaveText(stream);
}

export async function join(label: string): Promise<void> {
  await fill('join_label', label);
  if (device.getPlatform() === 'ios') {
    // The keyboard covers join_button on iOS. Dismiss it with the return
    // key on this plain (non-secure) field; doing the same on the secure
    // token field would make iOS offer to save it as a password.
    await element(by.id('join_label')).typeText('\n');
  }
  await tap('join_button');
  await expectText(`roster_${label}`, `${label} waiting`);
}
