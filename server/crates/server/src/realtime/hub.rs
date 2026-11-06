//! This instance's sockets, and which rooms, users and presence updates each
//! one receives.
//!
//! The tables sit behind a `std::sync::RwLock`, not an async lock: every
//! critical section is a few map operations, never an `.await`, so the lock
//! is held for microseconds and can't be held across a suspension point.
//! Delivery collects the target senders under the read lock, releases it,
//! and only then sends.
//!
//! Sending uses `try_send` on each socket's bounded queue. A full queue means
//! that client isn't reading fast enough; it is closed (it will reconnect and
//! sync) instead of slowing delivery to everyone else.

use std::{
    collections::{HashMap, HashSet},
    net::IpAddr,
    sync::RwLock,
};

use axum::extract::ws::Utf8Bytes;
use protocol::frames::{ServerFrame, close};
use storage::sessions::TokenHash;
use tokio::sync::{mpsc, watch};
use uuid::Uuid;

use crate::realtime::events;

pub type ConnId = Uuid;

/// Why a connection is being closed: code and reason for the close frame.
pub type CloseReason = (u16, &'static str);

/// The parts of a connection other tasks need.
pub struct ConnHandle {
    pub user_id: Uuid,
    pub ip: IpAddr,
    pub session_id: Uuid,
    pub token_hash: TokenHash,
    pub outbound: mpsc::Sender<Utf8Bytes>,
    /// Set to close the connection; the writer task watches it.
    pub closer: watch::Sender<Option<CloseReason>>,
}

struct Conn {
    handle: ConnHandle,
    rooms: HashSet<Uuid>,
    watching: HashSet<Uuid>,
}

#[derive(Default)]
struct Tables {
    conns: HashMap<ConnId, Conn>,
    rooms: HashMap<Uuid, HashSet<ConnId>>,
    users: HashMap<Uuid, HashSet<ConnId>>,
    /// Watched user -> connections watching them.
    watchers: HashMap<Uuid, HashSet<ConnId>>,
}

#[derive(Default)]
pub struct Hub {
    tables: RwLock<Tables>,
}

/// A delivery target: the connection, its queue, and its closer.
type Target = (
    ConnId,
    mpsc::Sender<Utf8Bytes>,
    watch::Sender<Option<CloseReason>>,
);

/// The result of a delivery, for tests and logs.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Delivered {
    pub sent: usize,
    pub too_slow: usize,
}

fn remove_from(index: &mut HashMap<Uuid, HashSet<ConnId>>, key: Uuid, conn: ConnId) -> bool {
    if let Some(set) = index.get_mut(&key) {
        set.remove(&conn);
        if set.is_empty() {
            index.remove(&key);
            return true;
        }
    }
    false
}

impl Hub {
    fn read(&self) -> std::sync::RwLockReadGuard<'_, Tables> {
        self.tables.read().expect("hub lock poisoned")
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, Tables> {
        self.tables.write().expect("hub lock poisoned")
    }

    /// Adds a connection, routed to `rooms`. Returns the channels this
    /// instance must now subscribe to (rooms and the user, if they are new
    /// here).
    pub fn register(&self, conn_id: ConnId, handle: ConnHandle, rooms: &[Uuid]) -> Vec<String> {
        let mut t = self.write();
        let mut channels = Vec::new();
        let user_id = handle.user_id;
        let users_entry = t.users.entry(user_id).or_default();
        if users_entry.is_empty() {
            channels.push(events::user_channel(user_id));
        }
        users_entry.insert(conn_id);
        for &room in rooms {
            let entry = t.rooms.entry(room).or_default();
            if entry.is_empty() {
                channels.push(events::room_channel(room));
            }
            entry.insert(conn_id);
        }
        t.conns.insert(
            conn_id,
            Conn {
                handle,
                rooms: rooms.iter().copied().collect(),
                watching: HashSet::new(),
            },
        );
        channels
    }

    /// Removes a connection. Returns the channels no local socket needs any
    /// more.
    pub fn unregister(&self, conn_id: ConnId) -> Vec<String> {
        let mut t = self.write();
        let Some(conn) = t.conns.remove(&conn_id) else {
            return Vec::new();
        };
        let mut released = Vec::new();
        if remove_from(&mut t.users, conn.handle.user_id, conn_id) {
            released.push(events::user_channel(conn.handle.user_id));
        }
        for room in conn.rooms {
            if remove_from(&mut t.rooms, room, conn_id) {
                released.push(events::room_channel(room));
            }
        }
        for watched in conn.watching {
            remove_from(&mut t.watchers, watched, conn_id);
        }
        released
    }

    /// Routes a room to every connection of `user_id`. Returns true if the
    /// room's channel is new to this instance.
    pub fn route_user_to_room(&self, user_id: Uuid, room_id: Uuid) -> bool {
        let mut t = self.write();
        let conns: Vec<ConnId> = t
            .users
            .get(&user_id)
            .into_iter()
            .flatten()
            .copied()
            .collect();
        if conns.is_empty() {
            return false;
        }
        let entry = t.rooms.entry(room_id).or_default();
        let new_channel = entry.is_empty();
        entry.extend(conns.iter().copied());
        for conn in conns {
            if let Some(c) = t.conns.get_mut(&conn) {
                c.rooms.insert(room_id);
            }
        }
        new_channel
    }

    /// Stops routing a room to `user_id`'s connections. Returns those
    /// connections' senders (to tell them) and whether the room's channel is
    /// no longer needed here.
    pub fn unroute_user_from_room(
        &self,
        user_id: Uuid,
        room_id: Uuid,
    ) -> (Vec<mpsc::Sender<Utf8Bytes>>, bool) {
        let mut t = self.write();
        let conns: Vec<ConnId> = t
            .users
            .get(&user_id)
            .into_iter()
            .flatten()
            .copied()
            .collect();
        let mut affected = Vec::new();
        let mut released = false;
        for conn in conns {
            let was_routed = t
                .conns
                .get_mut(&conn)
                .is_some_and(|c| c.rooms.remove(&room_id));
            if was_routed {
                released |= remove_from(&mut t.rooms, room_id, conn);
                if let Some(c) = t.conns.get(&conn) {
                    affected.push(c.handle.outbound.clone());
                }
            }
        }
        (affected, released)
    }

    /// Stops routing a room to anyone (it was deleted). Returns the senders
    /// of the connections that had it.
    pub fn drop_room(&self, room_id: Uuid) -> Vec<mpsc::Sender<Utf8Bytes>> {
        let mut t = self.write();
        let conns = t.rooms.remove(&room_id).unwrap_or_default();
        conns
            .into_iter()
            .filter_map(|conn| {
                let c = t.conns.get_mut(&conn)?;
                c.rooms.remove(&room_id);
                Some(c.handle.outbound.clone())
            })
            .collect()
    }

    /// Replaces the rooms routed to a user's connections (after reconnecting
    /// to Redis, from Postgres). Returns (rooms added, rooms removed).
    pub fn set_user_rooms(&self, user_id: Uuid, rooms: &HashSet<Uuid>) -> (Vec<Uuid>, Vec<Uuid>) {
        let current: HashSet<Uuid> = {
            let t = self.read();
            t.users
                .get(&user_id)
                .into_iter()
                .flatten()
                .filter_map(|conn| t.conns.get(conn))
                .flat_map(|c| c.rooms.iter().copied())
                .collect()
        };
        let added: Vec<Uuid> = rooms.difference(&current).copied().collect();
        let removed: Vec<Uuid> = current.difference(rooms).copied().collect();
        for &room in &added {
            self.route_user_to_room(user_id, room);
        }
        for &room in &removed {
            self.unroute_user_from_room(user_id, room);
        }
        (added, removed)
    }

    /// Whether any local socket needs a channel. The subscriber asks this
    /// before unsubscribing, so a release that raced with a new interest
    /// never unsubscribes a channel someone still needs.
    pub fn wants(&self, channel: &str) -> bool {
        let t = self.read();
        if channel == events::PRESENCE_CHANNEL {
            return true;
        }
        let parse = |prefix: &str| {
            channel
                .strip_prefix(prefix)
                .and_then(|id| id.parse::<Uuid>().ok())
        };
        if let Some(room) = parse("room:") {
            return t.rooms.contains_key(&room);
        }
        if let Some(user) = parse("user:") {
            return t.users.contains_key(&user);
        }
        false
    }

    /// Every channel this instance needs, for resubscribing after a
    /// reconnect.
    pub fn all_channels(&self) -> Vec<String> {
        let t = self.read();
        let mut channels = vec![events::PRESENCE_CHANNEL.to_string()];
        channels.extend(t.rooms.keys().map(|&room| events::room_channel(room)));
        channels.extend(t.users.keys().map(|&user| events::user_channel(user)));
        channels
    }

    /// (user, connection) for every local connection.
    pub fn connections(&self) -> Vec<(Uuid, ConnId)> {
        self.read()
            .conns
            .iter()
            .map(|(id, c)| (c.handle.user_id, *id))
            .collect()
    }

    /// (user, IP, connection) for every local connection, for refreshing the
    /// connection caps.
    pub fn connections_with_ip(&self) -> Vec<(Uuid, IpAddr, ConnId)> {
        self.read()
            .conns
            .iter()
            .map(|(id, c)| (c.handle.user_id, c.handle.ip, *id))
            .collect()
    }

    pub fn local_users(&self) -> Vec<Uuid> {
        self.read().users.keys().copied().collect()
    }

    /// (session ID, token hash) of every connection, deduplicated.
    pub fn sessions(&self) -> Vec<(Uuid, TokenHash)> {
        let t = self.read();
        let mut seen = HashSet::new();
        t.conns
            .values()
            .filter(|c| seen.insert(c.handle.session_id))
            .map(|c| (c.handle.session_id, c.handle.token_hash))
            .collect()
    }

    pub fn connection_count(&self) -> usize {
        self.read().conns.len()
    }

    pub fn deliver_to_room(&self, room_id: Uuid, frame: &Utf8Bytes) -> Delivered {
        let targets = self.collect(|t| t.rooms.get(&room_id));
        self.send_all(targets, frame)
    }

    pub fn deliver_to_user(&self, user_id: Uuid, frame: &Utf8Bytes) -> Delivered {
        let targets = self.collect(|t| t.users.get(&user_id));
        self.send_all(targets, frame)
    }

    pub fn deliver_to_watchers(&self, watched: Uuid, frame: &Utf8Bytes) -> Delivered {
        let targets = self.collect(|t| t.watchers.get(&watched));
        self.send_all(targets, frame)
    }

    pub fn deliver_to_all(&self, frame: &Utf8Bytes) -> Delivered {
        let targets: Vec<_> = {
            let t = self.read();
            t.conns
                .iter()
                .map(|(id, c)| (*id, c.handle.outbound.clone(), c.handle.closer.clone()))
                .collect()
        };
        self.send_all(targets, frame)
    }

    /// Replaces the set of users whose presence a connection receives.
    pub fn set_watching(&self, conn_id: ConnId, users: HashSet<Uuid>) {
        let mut t = self.write();
        let Some(conn) = t.conns.get_mut(&conn_id) else {
            return;
        };
        let old = std::mem::replace(&mut conn.watching, users.clone());
        for user in old.difference(&users) {
            remove_from(&mut t.watchers, *user, conn_id);
        }
        for user in users {
            t.watchers.entry(user).or_default().insert(conn_id);
        }
    }

    /// Closes every connection of a session.
    pub fn close_session(&self, session_id: Uuid, reason: CloseReason) -> usize {
        let t = self.read();
        let mut closed = 0;
        for conn in t
            .conns
            .values()
            .filter(|c| c.handle.session_id == session_id)
        {
            let _ = conn.handle.closer.send(Some(reason));
            closed += 1;
        }
        closed
    }

    pub fn close_all(&self, reason: CloseReason) {
        for conn in self.read().conns.values() {
            let _ = conn.handle.closer.send(Some(reason));
        }
    }

    fn collect(&self, select: impl Fn(&Tables) -> Option<&HashSet<ConnId>>) -> Vec<Target> {
        let t = self.read();
        select(&t)
            .into_iter()
            .flatten()
            .filter_map(|id| {
                let c = t.conns.get(id)?;
                Some((*id, c.handle.outbound.clone(), c.handle.closer.clone()))
            })
            .collect()
    }

    fn send_all(&self, targets: Vec<Target>, frame: &Utf8Bytes) -> Delivered {
        let mut delivered = Delivered::default();
        for (conn_id, outbound, closer) in targets {
            match outbound.try_send(frame.clone()) {
                Ok(()) => delivered.sent += 1,
                Err(mpsc::error::TrySendError::Full(_)) => {
                    tracing::info!(%conn_id, "closing slow connection");
                    let _ = closer.send(Some((close::TOO_SLOW, "too slow")));
                    delivered.too_slow += 1;
                }
                // The connection is shutting down already.
                Err(mpsc::error::TrySendError::Closed(_)) => {}
            }
        }
        delivered
    }
}

/// Serializes a frame for sending.
pub fn encode(frame: &ServerFrame) -> Utf8Bytes {
    Utf8Bytes::from(serde_json::to_string(frame).expect("server frames always serialize"))
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestConn {
        id: ConnId,
        rx: mpsc::Receiver<Utf8Bytes>,
        closed: watch::Receiver<Option<CloseReason>>,
    }

    fn connect(hub: &Hub, user: Uuid, rooms: &[Uuid], capacity: usize) -> (TestConn, Vec<String>) {
        let (tx, rx) = mpsc::channel(capacity);
        let (closer, closed) = watch::channel(None);
        let id = Uuid::now_v7();
        let channels = hub.register(
            id,
            ConnHandle {
                user_id: user,
                ip: IpAddr::from([127, 0, 0, 1]),
                session_id: Uuid::now_v7(),
                token_hash: [0; 32],
                outbound: tx,
                closer,
            },
            rooms,
        );
        (TestConn { id, rx, closed }, channels)
    }

    #[test]
    fn subscriptions_are_reference_counted() {
        let hub = Hub::default();
        let (user, room) = (Uuid::now_v7(), Uuid::now_v7());
        let (a, channels) = connect(&hub, user, &[room], 8);
        assert_eq!(channels.len(), 2, "user and room channels are new");
        let (b, channels) = connect(&hub, user, &[room], 8);
        assert!(channels.is_empty(), "second socket needs nothing new");
        assert!(hub.wants(&events::room_channel(room)));

        assert!(hub.unregister(a.id).is_empty(), "b still needs both");
        let released = hub.unregister(b.id);
        assert_eq!(released.len(), 2);
        assert!(!hub.wants(&events::room_channel(room)));
        assert!(!hub.wants(&events::user_channel(user)));
        assert!(hub.wants(events::PRESENCE_CHANNEL));
    }

    #[test]
    fn delivery_reaches_only_routed_connections() {
        let hub = Hub::default();
        let (room, other_room) = (Uuid::now_v7(), Uuid::now_v7());
        let (mut a, _) = connect(&hub, Uuid::now_v7(), &[room], 8);
        let (mut b, _) = connect(&hub, Uuid::now_v7(), &[other_room], 8);
        let frame = Utf8Bytes::from("hi");
        assert_eq!(hub.deliver_to_room(room, &frame).sent, 1);
        assert_eq!(a.rx.try_recv().unwrap(), "hi");
        assert!(b.rx.try_recv().is_err());
    }

    #[test]
    fn a_full_queue_closes_that_connection_only() {
        let hub = Hub::default();
        let room = Uuid::now_v7();
        let (slow, _) = connect(&hub, Uuid::now_v7(), &[room], 2);
        let (mut fast, _) = connect(&hub, Uuid::now_v7(), &[room], 100);
        let frame = Utf8Bytes::from("x");
        let mut too_slow = 0;
        for _ in 0..5 {
            let d = hub.deliver_to_room(room, &frame);
            too_slow += d.too_slow;
            // The fast client keeps reading.
            assert_eq!(fast.rx.try_recv().unwrap(), "x");
        }
        assert!(too_slow >= 1);
        assert_eq!(*slow.closed.borrow(), Some((close::TOO_SLOW, "too slow")));
        assert_eq!(*fast.closed.borrow(), None);
    }

    #[test]
    fn removal_unroutes_every_connection_of_the_user() {
        let hub = Hub::default();
        let (user, room) = (Uuid::now_v7(), Uuid::now_v7());
        let (mut a, _) = connect(&hub, user, &[room], 8);
        let (mut b, _) = connect(&hub, user, &[room], 8);
        let (mut other, _) = connect(&hub, Uuid::now_v7(), &[room], 8);

        let (affected, released) = hub.unroute_user_from_room(user, room);
        assert_eq!(affected.len(), 2);
        assert!(!released, "the other user still needs the room");
        hub.deliver_to_room(room, &Utf8Bytes::from("after"));
        assert!(a.rx.try_recv().is_err());
        assert!(b.rx.try_recv().is_err());
        assert_eq!(other.rx.try_recv().unwrap(), "after");
    }

    #[test]
    fn new_rooms_route_to_every_connection_of_the_user() {
        let hub = Hub::default();
        let user = Uuid::now_v7();
        let (mut a, _) = connect(&hub, user, &[], 8);
        let (mut b, _) = connect(&hub, user, &[], 8);
        let room = Uuid::now_v7();
        assert!(hub.route_user_to_room(user, room));
        assert!(!hub.route_user_to_room(user, room), "already subscribed");
        hub.deliver_to_room(room, &Utf8Bytes::from("hi"));
        assert!(a.rx.try_recv().is_ok() && b.rx.try_recv().is_ok());
        // A user with no sockets here: nothing to route.
        assert!(!hub.route_user_to_room(Uuid::now_v7(), room));
    }

    #[test]
    fn set_user_rooms_reconciles_both_ways() {
        let hub = Hub::default();
        let user = Uuid::now_v7();
        let (kept, gone, new) = (Uuid::now_v7(), Uuid::now_v7(), Uuid::now_v7());
        let (_a, _) = connect(&hub, user, &[kept, gone], 8);
        let (added, removed) = hub.set_user_rooms(user, &[kept, new].into_iter().collect());
        assert_eq!(added, [new]);
        assert_eq!(removed, [gone]);
        assert!(hub.wants(&events::room_channel(new)));
        assert!(!hub.wants(&events::room_channel(gone)));
    }

    #[test]
    fn watchers_get_presence() {
        let hub = Hub::default();
        let watched = Uuid::now_v7();
        let (mut a, _) = connect(&hub, Uuid::now_v7(), &[], 8);
        hub.set_watching(a.id, [watched].into_iter().collect());
        assert_eq!(
            hub.deliver_to_watchers(watched, &Utf8Bytes::from("p")).sent,
            1
        );
        assert_eq!(a.rx.try_recv().unwrap(), "p");
        hub.set_watching(a.id, HashSet::new());
        assert_eq!(
            hub.deliver_to_watchers(watched, &Utf8Bytes::from("p")).sent,
            0
        );
    }
}
