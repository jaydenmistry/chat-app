// Small presentational pieces shared by the chat components.

import type { PresenceStatus } from "../protocol/generated/PresenceStatus";
import type { RoomInfo } from "../protocol/generated/RoomInfo";

export function roomLabel(room: RoomInfo): string {
  return room.kind === "dm" ? (room.dm_peer?.display_name ?? "Direct message") : `# ${room.name ?? ""}`;
}

export function PresenceDot({ status }: { status: PresenceStatus | undefined }) {
  const s = status ?? "offline";
  return <span className={`presence presence-${s}`} role="img" aria-label={s} title={s} />;
}
