import { type FormEvent, useEffect, useState } from "react";
import { ApiRequestError, GITHUB_START_URL, api } from "./api";
import type { Me } from "./protocol/generated/Me";

type Mode = "login" | "signup";

/** Field-level errors from `invalid_input` and `conflict` responses. */
type FieldErrors = Partial<Record<"email" | "password" | "handle" | "display_name", string>>;

export function SignIn({ onSignedIn }: { onSignedIn: (me: Me) => void }) {
  const [mode, setMode] = useState<Mode>("login");
  const [github, setGithub] = useState(false);
  const [busy, setBusy] = useState(false);
  const [fieldErrors, setFieldErrors] = useState<FieldErrors>({});
  const [formError, setFormError] = useState<string | null>(() =>
    new URLSearchParams(window.location.search).get("auth_error") === "github"
      ? "GitHub sign-in didn't complete. Please try again."
      : null,
  );

  useEffect(() => {
    api.authConfig().then(
      (config) => setGithub(config.github),
      () => setGithub(false),
    );
  }, []);

  async function run(action: () => Promise<Me>) {
    setBusy(true);
    setFieldErrors({});
    setFormError(null);
    try {
      const me = await action();
      // Drop ?auth_error from the URL once signed in.
      window.history.replaceState(null, "", "/");
      onSignedIn(me);
    } catch (err) {
      if (err instanceof ApiRequestError && err.body.field) {
        setFieldErrors({ [err.body.field]: err.body.message });
      } else {
        setFormError(err instanceof Error ? err.message : String(err));
      }
    } finally {
      setBusy(false);
    }
  }

  function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const form = new FormData(event.currentTarget);
    const value = (name: string) => String(form.get(name) ?? "");
    if (mode === "login") {
      void run(() => api.login({ email: value("email"), password: value("password") }));
    } else {
      void run(() =>
        api.signup({
          email: value("email"),
          password: value("password"),
          handle: value("handle"),
          display_name: value("display_name"),
        }),
      );
    }
  }

  return (
    <main className="centered">
      <section className="card" aria-labelledby="signin-title">
        <h1 id="signin-title">{mode === "login" ? "Sign in" : "Create an account"}</h1>

        <form onSubmit={submit} noValidate>
          {mode === "signup" && (
            <>
              <Field name="display_name" label="Display name" autoComplete="name" error={fieldErrors.display_name} />
              <Field name="handle" label="Handle" autoComplete="username" error={fieldErrors.handle} />
            </>
          )}
          <Field name="email" label="Email" type="email" autoComplete="email" error={fieldErrors.email} />
          <Field
            name="password"
            label="Password"
            type="password"
            autoComplete={mode === "login" ? "current-password" : "new-password"}
            error={fieldErrors.password}
          />
          {formError && (
            <p className="error" role="alert">
              {formError}
            </p>
          )}
          <button type="submit" disabled={busy}>
            {mode === "login" ? "Sign in" : "Create account"}
          </button>
        </form>

        <p className="switch">
          {mode === "login" ? "No account yet? " : "Already have an account? "}
          <button
            type="button"
            className="link"
            onClick={() => {
              setMode(mode === "login" ? "signup" : "login");
              setFieldErrors({});
              setFormError(null);
            }}
          >
            {mode === "login" ? "Create one" : "Sign in"}
          </button>
        </p>

        <div className="alternatives">
          {github && (
            <a className="button secondary" href={GITHUB_START_URL}>
              Sign in with GitHub
            </a>
          )}
          <button type="button" className="secondary" disabled={busy} onClick={() => void run(api.guest)}>
            Try as guest
          </button>
          <p className="hint">Guest accounts and their messages are deleted after 24 hours.</p>
        </div>
      </section>
    </main>
  );
}

function Field({
  name,
  label,
  type = "text",
  autoComplete,
  error,
}: {
  name: string;
  label: string;
  type?: string;
  autoComplete: string;
  error: string | undefined;
}) {
  const inputId = `field-${name}`;
  const errorId = `${inputId}-error`;
  // The error sits outside the <label>, so it isn't read as part of the
  // field's name; aria-describedby links it to the input instead.
  return (
    <div className="field">
      <label htmlFor={inputId}>{label}</label>
      <input
        id={inputId}
        name={name}
        type={type}
        autoComplete={autoComplete}
        aria-invalid={error ? true : undefined}
        aria-describedby={error ? errorId : undefined}
      />
      {error && (
        <span id={errorId} className="error">
          {error}
        </span>
      )}
    </div>
  );
}
