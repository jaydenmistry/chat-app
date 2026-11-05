/** Unwraps a value the test expects to exist, failing loudly if it doesn't. */
export function must<T>(value: T | undefined | null): T {
  if (value === undefined || value === null) throw new Error("expected a value");
  return value;
}
