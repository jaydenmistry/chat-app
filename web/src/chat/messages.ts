// Merging messages from history, sync and live delivery into one ordered
// list per room. The same message can arrive several ways (the sender's own
// fan-out copy, a sync overlap, a retry's duplicate ack), so everything goes
// through `mergeMessages`: deduplicate by ID, keep the newest version, order
// by ID.

import type { Message } from "../protocol/generated/Message";

/**
 * Orders two message IDs. IDs are decimal strings of non-negative 64-bit
 * integers without leading zeros, so a shorter string is a smaller number
 * and equal lengths compare as text. No BigInt needed.
 */
export function compareIds(a: string, b: string): number {
  if (a.length !== b.length) return a.length - b.length;
  return a < b ? -1 : a > b ? 1 : 0;
}

/**
 * Of two copies of the same message (`a` held, `b` arriving), the one to
 * keep. A deleted copy always wins: once deleted, nothing that arrives later
 * can bring the content back. Otherwise the higher version wins, and on a
 * tie the arriving copy: reactions change without changing the version, so
 * a fresher copy of the same version may carry newer reactions.
 */
export function newerCopy(a: Message, b: Message): Message {
  if (a.deleted !== b.deleted) return a.deleted ? a : b;
  return b.version >= a.version ? b : a;
}

/** Returns a new list, oldest first, with `incoming` merged in. */
export function mergeMessages(existing: readonly Message[], incoming: readonly Message[]): Message[] {
  if (incoming.length === 0) return existing as Message[];
  const byId = new Map<string, Message>();
  for (const m of existing) byId.set(m.id, m);
  for (const m of incoming) {
    const current = byId.get(m.id);
    byId.set(m.id, current ? newerCopy(current, m) : m);
  }
  return [...byId.values()].sort((x, y) => compareIds(x.id, y.id));
}

/** The highest ID in a list, for sync cursors. */
export function lastId(messages: readonly Message[]): string | null {
  return messages.at(-1)?.id ?? null;
}
