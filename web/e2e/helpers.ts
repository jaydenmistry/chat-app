// Shared steps for the end-to-end tests.

import { type Browser, type Page, expect } from "@playwright/test";

export function unique(prefix: string): string {
  return `${prefix}_${Date.now().toString(36)}${Math.floor(Math.random() * 1e6).toString(36)}`;
}

export async function signUp(browser: Browser, handle: string): Promise<Page> {
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

export async function createRoom(page: Page, name: string) {
  await page.getByRole("button", { name: "New room" }).click();
  await page.getByLabel("Name").fill(name);
  await page.getByRole("button", { name: "Create room" }).click();
  await expect(page.getByRole("heading", { level: 1 })).toHaveText(`# ${name}`);
}

export async function joinRoom(page: Page, name: string) {
  await page.getByRole("button", { name: "Browse rooms" }).click();
  const dialog = page.getByRole("dialog", { name: "Public rooms" });
  await dialog.getByRole("listitem").filter({ hasText: `# ${name}` }).getByRole("button", { name: "Join" }).click();
  await expect(page.getByRole("heading", { level: 1 })).toHaveText(`# ${name}`);
}

export async function send(page: Page, text: string) {
  const box = page.getByRole("textbox", { name: /^Message/ });
  await box.fill(text);
  await box.press("Enter");
}

export function messages(page: Page) {
  return page.getByRole("log", { name: "Messages" }).locator("article .content");
}
