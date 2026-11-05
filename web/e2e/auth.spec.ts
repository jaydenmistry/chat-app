import { type Page, expect, test } from "@playwright/test";

// Runs against the full stack: Vite, Traefik and both server instances. Each
// test makes its own accounts, so tests can run in parallel and repeatedly.

function unique(prefix: string): string {
  return `${prefix}_${Date.now().toString(36)}${Math.floor(Math.random() * 1e6).toString(36)}`;
}

async function createAccount(page: Page, handle: string, email: string, password: string) {
  await page.goto("/");
  await page.getByRole("button", { name: "Create one" }).click();
  await page.getByLabel("Display name").fill("E2E User");
  await page.getByLabel("Handle").fill(handle);
  await page.getByLabel("Email").fill(email);
  await page.getByLabel("Password").fill(password);
  await page.getByRole("button", { name: "Create account" }).click();
}

/** Opens the account page from the chat sidebar (the user's name). */
async function openAccount(page: Page) {
  await page.getByRole("complementary", { name: "Rooms" }).locator(".sidebar-foot button").click();
}

test("try as guest, then sign out", async ({ page }) => {
  await page.goto("/");
  await page.getByRole("button", { name: "Try as guest" }).click();
  await expect(page.getByText("You're not in any rooms yet.")).toBeVisible();

  await openAccount(page);
  await expect(page.getByRole("heading", { level: 1 })).toHaveText(/^Guest /);
  await expect(page.getByText(/This guest account and its messages are deleted at/)).toBeVisible();
  await expect(page.getByText("this device")).toBeVisible();

  // The session survives a reload: the cookie works through Vite and Traefik.
  await page.reload();
  await openAccount(page);
  await expect(page.getByRole("heading", { level: 1 })).toHaveText(/^Guest /);

  await page.getByRole("button", { name: "Sign out" }).last().click();
  await expect(page.getByRole("heading", { name: "Sign in" })).toBeVisible();
  await page.reload();
  await expect(page.getByRole("heading", { name: "Sign in" })).toBeVisible();
});

test("create an account, sign out, sign back in", async ({ page }) => {
  const handle = unique("e2e");
  const email = `${handle}@example.com`;
  const password = "correct horse battery";

  await createAccount(page, handle, email, password);
  await expect(page.getByText(`@${handle}`)).toBeVisible();
  await openAccount(page);
  await page.getByRole("button", { name: "Sign out" }).last().click();

  await page.getByLabel("Email").fill(email);
  await page.getByLabel("Password").fill("wrong password");
  await page.getByRole("button", { name: "Sign in" }).click();
  await expect(page.getByRole("alert")).toHaveText("wrong email or password");

  await page.getByLabel("Password").fill(password);
  await page.getByRole("button", { name: "Sign in" }).click();
  await expect(page.getByText(`@${handle}`)).toBeVisible();
});

test("a taken handle is reported on the handle field", async ({ page, browser }) => {
  const handle = unique("e2e");
  await createAccount(page, handle, `${handle}@example.com`, "correct horse battery");
  await expect(page.getByText(`@${handle}`)).toBeVisible();

  // A second, signed-out browser tries the same handle.
  const other = await browser.newPage();
  await createAccount(other, handle, `${handle}.2@example.com`, "correct horse battery");
  await expect(other.getByText("this handle is taken")).toBeVisible();
  await expect(other.getByLabel("Handle")).toHaveAttribute("aria-invalid", "true");
  await other.close();
});

test("revoking a session signs that browser out", async ({ page, browser }) => {
  const handle = unique("e2e");
  const email = `${handle}@example.com`;
  const password = "correct horse battery";
  await createAccount(page, handle, email, password);
  await expect(page.getByText(`@${handle}`)).toBeVisible();

  // A second browser context has its own cookies: a second device.
  const phoneContext = await browser.newContext();
  const phone = await phoneContext.newPage();
  await phone.goto("/");
  await phone.getByLabel("Email").fill(email);
  await phone.getByLabel("Password").fill(password);
  await phone.getByRole("button", { name: "Sign in" }).click();
  await expect(phone.getByText(`@${handle}`)).toBeVisible();

  await page.reload();
  await openAccount(page);
  await page.getByRole("button", { name: "Revoke" }).click();
  await expect(page.getByRole("button", { name: "Revoke" })).toHaveCount(0);

  // The revoked browser's socket is closed (4001) and it returns to sign-in.
  await expect(phone.getByRole("heading", { name: "Sign in" })).toBeVisible();
  await phoneContext.close();
});
