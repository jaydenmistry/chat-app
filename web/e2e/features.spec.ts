import { type Page, expect, test } from "@playwright/test";
import { createRoom, joinRoom, messages, send, signUp, unique } from "./helpers";

// Phase 3 features between two people on the full stack.

function log(page: Page) {
  return page.getByRole("log", { name: "Messages" });
}

function roomButton(page: Page, name: string) {
  return page.getByRole("navigation").getByRole("button", { name: new RegExp(name) });
}

/** A word no other test run will have sent, letters only so it's one search term. */
function uniqueWord(): string {
  const letters = "abcdefghijklmnopqrstuvwxyz";
  return Array.from({ length: 10 }, () => letters[Math.floor(Math.random() * letters.length)]).join("");
}

test("edits, deletes and reactions reach the other person", async ({ browser }) => {
  const room = unique("room");
  const ada = await signUp(browser, unique("ada"));
  const bob = await signUp(browser, unique("bob"));
  await createRoom(ada, room);
  await joinRoom(bob, room);
  await send(ada, "helo");
  await expect(messages(bob)).toHaveText(["helo"]);

  const adaMessage = log(ada).locator("article").first();
  await adaMessage.hover();
  await adaMessage.getByRole("button", { name: "Edit" }).click();
  const box = ada.getByRole("textbox", { name: "Edit message" });
  await box.fill("hello");
  await box.press("Enter");
  await expect(messages(bob)).toHaveText(["hello"]);
  await expect(log(bob).getByText("(edited)")).toBeVisible();

  const bobView = log(bob).locator("article").first();
  await bobView.hover();
  await bobView.getByRole("button", { name: "Add reaction" }).click();
  await bobView.getByRole("group", { name: "Reactions" }).getByRole("button", { name: "👍", exact: true }).click();
  await expect(ada.getByRole("button", { name: "👍 1" })).toHaveAttribute("aria-pressed", "false");
  await ada.getByRole("button", { name: "👍 1" }).click();
  await expect(bob.getByRole("button", { name: "👍 2" })).toHaveAttribute("aria-pressed", "true");
  await expect(ada.getByRole("button", { name: "👍 2" })).toHaveAttribute("aria-pressed", "true");

  // Reactions and edits are stored, not just relayed.
  await bob.reload();
  await expect(messages(bob)).toHaveText(["hello"]);
  await expect(bob.getByRole("button", { name: "👍 2" })).toBeVisible();

  ada.once("dialog", (dialog) => void dialog.accept());
  await adaMessage.hover();
  await adaMessage.getByRole("button", { name: "Delete" }).click();
  await expect(messages(bob)).toHaveText(["Message deleted"]);
  await expect(bob.getByRole("button", { name: /👍/ })).toHaveCount(0);
});

test("unread badges clear on reading, and senders see who read", async ({ browser }) => {
  test.setTimeout(60_000);
  const general = unique("room");
  const other = unique("room");
  const adaHandle = unique("ada");
  const bobHandle = unique("bob");
  const ada = await signUp(browser, adaHandle);
  const bob = await signUp(browser, bobHandle);
  await createRoom(ada, general);
  await createRoom(ada, other);
  await joinRoom(bob, general);
  await joinRoom(bob, other);
  await roomButton(bob, general).click();

  await send(ada, "anyone here?");
  await expect(roomButton(bob, other).getByLabel("1 unread")).toBeVisible();
  await roomButton(bob, other).click();
  await expect(messages(bob)).toHaveText(["anyone here?"]);
  await expect(roomButton(bob, other).getByLabel(/unread/)).toHaveCount(0);

  // "Seen by" is fetched on demand: hovering your latest message refreshes it.
  const adaMessage = log(ada).locator("article").first();
  await expect(async () => {
    await ada.mouse.move(0, 0);
    await adaMessage.hover();
    await expect(adaMessage.getByText("Seen by 1")).toBeVisible({ timeout: 1_000 });
  }).toPass({ timeout: 15_000 });

  // The read position is stored: still read after a reload.
  await bob.reload();
  await expect(roomButton(bob, general)).toBeVisible();
  await expect(roomButton(bob, other).getByLabel(/unread/)).toHaveCount(0);

  // In a DM, the other person's read shows up live.
  await ada.getByRole("button", { name: "New message" }).click();
  await ada.getByLabel("Their handle").fill(bobHandle);
  await ada.getByRole("button", { name: "Open conversation" }).click();
  await send(ada, "ping");
  await expect(roomButton(bob, adaHandle).getByLabel("1 unread")).toBeVisible();
  await roomButton(bob, adaHandle).click();
  await expect(messages(bob)).toHaveText(["ping"]);
  await expect(log(ada).getByText("Seen", { exact: true })).toBeVisible();
});

test("search finds messages in your rooms only", async ({ browser }) => {
  test.setTimeout(60_000);
  const room = unique("room");
  const word = uniqueWord();
  const ada = await signUp(browser, unique("ada"));
  const bob = await signUp(browser, unique("bob"));
  await createRoom(ada, room);
  await send(ada, `the ${word} is ripe`);
  await createRoom(ada, unique("room"));

  const search = async (page: Page) => {
    const box = page.getByRole("searchbox", { name: "Search messages" });
    await box.fill(word);
    await box.press("Enter");
  };
  const results = (page: Page) => page.getByLabel("Search results").getByRole("listitem");

  // The index is a few seconds behind the messages.
  await expect(async () => {
    await search(ada);
    await expect(results(ada)).toHaveCount(1, { timeout: 1_000 });
  }).toPass({ timeout: 30_000 });
  await expect(results(ada)).toContainText(`the ${word} is ripe`);

  await search(bob);
  await expect(bob.getByText("No messages match")).toBeVisible();

  await results(ada).getByRole("button").click();
  await expect(ada.getByRole("heading", { level: 1 })).toHaveText(`# ${room}`);
});
