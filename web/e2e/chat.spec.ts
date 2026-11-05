import { type Browser, type Page, expect, test } from "@playwright/test";

// Two people, two browser contexts (separate cookies, like separate
// devices), against the full stack: Vite, Traefik, two server instances.
// Traefik balances each new connection, so the two users usually end up on
// different instances.

function unique(prefix: string): string {
  return `${prefix}_${Date.now().toString(36)}${Math.floor(Math.random() * 1e6).toString(36)}`;
}

async function signUp(browser: Browser, handle: string): Promise<Page> {
  const context = await browser.newContext();
  const page = await context.newPage();
  await page.goto("/");
  await page.getByRole("button", { name: "Create one" }).click();
  await page.getByLabel("Display name").fill(handle);
  await page.getByLabel("Handle").fill(handle);
  await page.getByLabel("Email").fill(`${handle}@example.com`);
  await page.getByLabel("Password").fill("correct horse battery");
  await page.getByRole("button", { name: "Create account" }).click();
  await expect(page.getByRole("status").filter({ hasText: "Connected" })).toBeVisible();
  return page;
}

async function createRoom(page: Page, name: string) {
  await page.getByRole("button", { name: "New room" }).click();
  await page.getByLabel("Name").fill(name);
  await page.getByRole("button", { name: "Create room" }).click();
  await expect(page.getByRole("heading", { level: 1 })).toHaveText(`# ${name}`);
}

async function joinRoom(page: Page, name: string) {
  await page.getByRole("button", { name: "Browse rooms" }).click();
  const dialog = page.getByRole("dialog", { name: "Public rooms" });
  await dialog.getByRole("listitem").filter({ hasText: `# ${name}` }).getByRole("button", { name: "Join" }).click();
  await expect(page.getByRole("heading", { level: 1 })).toHaveText(`# ${name}`);
}

async function send(page: Page, text: string) {
  const box = page.getByRole("textbox", { name: /^Message/ });
  await box.fill(text);
  await box.press("Enter");
}

function messages(page: Page) {
  return page.getByRole("log", { name: "Messages" }).locator("article .content");
}

test("two users chat in real time", async ({ browser }) => {
  const room = unique("room");
  const ada = await signUp(browser, unique("ada"));
  const bob = await signUp(browser, unique("bob"));
  await createRoom(ada, room);
  await joinRoom(bob, room);

  await send(ada, "hello bob");
  await expect(messages(bob)).toHaveText(["hello bob"]);
  await send(bob, "hi ada\nsecond line");
  await expect(messages(ada)).toHaveText(["hello bob", "hi ada\nsecond line"]);
  // Each sees the message once, though the sender also gets an ack and a
  // fan-out copy.
  await expect(messages(bob)).toHaveCount(2);
});

test("typing indicator and presence", async ({ browser }) => {
  const room = unique("room");
  const adaHandle = unique("ada");
  const bobHandle = unique("bob");
  const ada = await signUp(browser, adaHandle);
  const bob = await signUp(browser, bobHandle);
  await createRoom(ada, room);
  await joinRoom(bob, room);

  await bob.getByRole("textbox", { name: /^Message/ }).pressSequentially("typing…");
  await expect(ada.getByText(`${bobHandle} is typing…`)).toBeVisible();

  // A DM shows the other person's presence.
  await ada.getByRole("button", { name: "New message" }).click();
  await ada.getByLabel("Their handle").fill(bobHandle);
  await ada.getByRole("button", { name: "Open conversation" }).click();
  const dot = ada.getByRole("heading", { level: 1 }).getByRole("img");
  await expect(dot).toHaveAccessibleName("online");

  await bob.context().close();
  await expect(dot).toHaveAccessibleName("offline");
});

test("messages sent while offline arrive after reconnecting", async ({ browser }) => {
  test.setTimeout(90_000);
  const room = unique("room");
  const ada = await signUp(browser, unique("ada"));
  const bob = await signUp(browser, unique("bob"));
  await createRoom(ada, room);
  await joinRoom(bob, room);
  await send(ada, "before");
  await expect(messages(bob)).toHaveText(["before"]);

  await bob.context().setOffline(true);
  await expect(bob.getByRole("status").filter({ hasText: "Reconnecting…" })).toBeVisible();
  await send(ada, "while you were offline");
  // Bob writes too; it waits in his client until he's back.
  await send(bob, "written offline");
  await expect(bob.getByText("sending…")).toBeVisible();

  await bob.context().setOffline(false);
  await expect(bob.getByRole("status").filter({ hasText: "Connected" })).toBeVisible({ timeout: 40_000 });
  await expect(messages(bob)).toHaveText(["before", "while you were offline", "written offline"]);
  await expect(messages(ada)).toHaveText(["before", "while you were offline", "written offline"]);
});

test("history scrolls back past the first page", async ({ browser }) => {
  const room = unique("room");
  const ada = await signUp(browser, unique("ada"));
  await createRoom(ada, room);
  for (let i = 0; i < 55; i++) {
    await send(ada, `message ${i}`);
  }
  await expect(messages(ada)).toHaveCount(55);

  // A fresh page load shows the latest 50; older ones load on request.
  await ada.reload();
  await expect(messages(ada)).toHaveCount(50);
  await ada.getByRole("button", { name: "Load older messages" }).click();
  await expect(messages(ada)).toHaveCount(55);
  await expect(messages(ada).first()).toHaveText("message 0");
  await expect(ada.getByRole("button", { name: "Load older messages" })).toHaveCount(0);
});
