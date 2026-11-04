import { useEffect, useState } from "react";
import { ApiRequestError, api } from "./api";
import { Account } from "./Account";
import type { Me } from "./protocol/generated/Me";
import { SignIn } from "./SignIn";

type State = { kind: "loading" } | { kind: "signed-out" } | { kind: "signed-in"; me: Me } | { kind: "error"; message: string };

export function App() {
  const [state, setState] = useState<State>({ kind: "loading" });

  useEffect(() => {
    api.me().then(
      (me) => setState({ kind: "signed-in", me }),
      (err: unknown) => {
        if (err instanceof ApiRequestError && err.status === 401) {
          setState({ kind: "signed-out" });
        } else {
          setState({ kind: "error", message: err instanceof Error ? err.message : String(err) });
        }
      },
    );
  }, []);

  switch (state.kind) {
    case "loading":
      return <main className="centered">Loading…</main>;
    case "error":
      return (
        <main className="centered">
          <p role="alert">Something went wrong: {state.message}</p>
        </main>
      );
    case "signed-out":
      return <SignIn onSignedIn={(me) => setState({ kind: "signed-in", me })} />;
    case "signed-in":
      return <Account me={state.me} onSignedOut={() => setState({ kind: "signed-out" })} />;
  }
}
