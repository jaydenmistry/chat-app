//! The Scylla message store against real Scylla.

use std::{sync::Arc, time::Duration};

use protocol::ids::{BUCKET_MS, MessageId};
use storage::messages::{MessageStore, NewMessage, page_back, read_forward};
use uuid::Uuid;

use crate::infra::{Infra, KEYSPACE};

async fn store(infra: &Infra) -> MessageStore {
    let session =
        storage::scylladb::connect(std::slice::from_ref(&infra.scylla_node), Some(KEYSPACE))
            .await
            .unwrap();
    MessageStore::new(session).await.unwrap()
}

fn id_at(hour: u64, n: u16) -> MessageId {
    MessageId::from_parts(hour * BUCKET_MS + u64::from(n), 1, n)
}

async fn insert(
    store: &MessageStore,
    room: Uuid,
    id: MessageId,
    content: &str,
    ttl: Option<Duration>,
) {
    store
        .insert(&NewMessage {
            room_id: room,
            id,
            author_id: Uuid::nil(),
            content,
            ttl,
        })
        .await
        .unwrap();
}

/// Messages spread over hours with gaps page back completely, newest first,
/// across real partitions; forward reads return the tail oldest first.
pub async fn history_pages_across_real_buckets(infra: Arc<Infra>) {
    let store = store(&infra).await;
    let room = Uuid::now_v7();
    let mut expected = Vec::new();
    for (hour, count) in [(10, 3), (11, 7), (40, 1), (2000, 5)] {
        for n in 0..count {
            let id = id_at(hour, n);
            insert(&store, room, id, &format!("{hour}-{n}"), None).await;
            expected.push(id);
        }
    }
    expected.sort_unstable_by(|a, b| b.cmp(a));

    let floor = MessageId::from_parts(0, 0, 0);
    let mut seen = Vec::new();
    let mut before = None;
    loop {
        let page = page_back(&store, room, before, floor, 4).await.unwrap();
        seen.extend(page.messages.iter().map(|m| m.id));
        match page.next_before {
            Some(next) => before = Some(next),
            None => break,
        }
    }
    assert_eq!(seen, expected);

    let tail = read_forward(&store, room, id_at(11, 4), floor, 100)
        .await
        .unwrap();
    let tail_ids: Vec<_> = tail.messages.iter().map(|m| m.id).collect();
    let mut want: Vec<_> = expected
        .iter()
        .copied()
        .filter(|id| *id > id_at(11, 4))
        .collect();
    want.sort_unstable();
    assert_eq!(tail_ids, want);
    assert!(tail.complete);
    assert_eq!(tail.messages[0].content, "11-5");
}

/// Replaying an insert (what a send retry does) changes nothing.
pub async fn insert_is_idempotent(infra: Arc<Infra>) {
    let store = store(&infra).await;
    let room = Uuid::now_v7();
    let id = id_at(100, 0);
    for _ in 0..3 {
        insert(&store, room, id, "once", None).await;
    }
    let page = page_back(&store, room, None, MessageId::from_parts(0, 0, 0), 10)
        .await
        .unwrap();
    assert_eq!(page.messages.len(), 1);
    let stored = store.get(room, id).await.unwrap().unwrap();
    assert_eq!(stored.content, "once");
    assert_eq!(stored.version, id.write_timestamp_us());
}

/// A row written with a TTL disappears; its bucket stays in the index and
/// pagination steps over it.
pub async fn expired_messages_leave_empty_buckets(infra: Arc<Infra>) {
    let store = store(&infra).await;
    let room = Uuid::now_v7();
    insert(&store, room, id_at(1, 0), "keeps", None).await;
    insert(
        &store,
        room,
        id_at(5, 0),
        "expires",
        Some(Duration::from_secs(1)),
    )
    .await;
    insert(&store, room, id_at(9, 0), "keeps too", None).await;
    tokio::time::sleep(Duration::from_millis(2500)).await;

    assert_eq!(store.get(room, id_at(5, 0)).await.unwrap(), None);
    let page = page_back(&store, room, None, MessageId::from_parts(0, 0, 0), 10)
        .await
        .unwrap();
    let contents: Vec<_> = page.messages.iter().map(|m| m.content.as_str()).collect();
    assert_eq!(contents, ["keeps too", "keeps"]);
    assert_eq!(page.next_before, None);
}
