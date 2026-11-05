//! Worker-ID leases against real Redis.

use std::{sync::Arc, time::Duration};

use server::ids::{IdGenerator, LeaseTimings, run_lease};
use tokio::sync::watch;
use uuid::Uuid;

use crate::infra::Infra;

const FAST: LeaseTimings = LeaseTimings {
    ttl: Duration::from_secs(3),
    renew_every: Duration::from_millis(300),
    quarantine: Duration::ZERO,
    max_clock_skew: Duration::from_millis(500),
};

struct Instance {
    generator: Arc<IdGenerator>,
    stop: watch::Sender<bool>,
    task: tokio::task::JoinHandle<()>,
}

async fn start(infra: &Infra) -> Instance {
    start_with(infra, FAST).await
}

async fn start_with(infra: &Infra, timings: LeaseTimings) -> Instance {
    let redis = storage::redisdb::connect(&infra.redis_url).await.unwrap();
    let generator = Arc::new(IdGenerator::new(0));
    let (stop, stopped) = watch::channel(false);
    let task = tokio::spawn(run_lease(
        Arc::clone(&generator),
        redis,
        Uuid::now_v7(),
        timings,
        stopped,
    ));
    wait_ready(&generator).await;
    Instance {
        generator,
        stop,
        task,
    }
}

async fn wait_ready(generator: &IdGenerator) {
    for _ in 0..100 {
        if generator.is_ready() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("lease never became ready");
}

pub async fn instances_get_distinct_worker_ids(infra: Arc<Infra>) {
    let mut instances = Vec::new();
    for _ in 0..5 {
        instances.push(start(&infra).await);
    }
    let mut workers: Vec<u16> = instances
        .iter()
        .map(|i| i.generator.worker().unwrap())
        .collect();
    workers.sort_unstable();
    workers.dedup();
    assert_eq!(workers.len(), 5, "worker IDs collided: {workers:?}");
    for instance in &instances {
        let id = instance.generator.next_id().unwrap();
        assert_eq!(Some(id.worker()), instance.generator.worker());
    }
}

/// If the key stops holding our instance ID (someone else got it after a
/// Redis restart), the next renewal stops ID issue and the instance moves to
/// a different worker ID.
pub async fn a_stolen_lease_stops_ids_and_moves_on(infra: Arc<Infra>) {
    let instance = start(&infra).await;
    let original = instance.generator.worker().unwrap();
    let mut redis = storage::redisdb::connect(&infra.redis_url).await.unwrap();
    let _: () = redis::cmd("SET")
        .arg(format!("worker:{original}"))
        .arg("someone-else")
        .arg("PX")
        .arg(30_000)
        .query_async(&mut redis)
        .await
        .unwrap();

    let mut moved = None;
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        if let Some(worker) = instance.generator.worker()
            && worker != original
            && instance.generator.is_ready()
        {
            moved = Some(worker);
            break;
        }
    }
    let moved = moved.expect("instance never moved to a new worker ID");
    assert_ne!(moved, original);
    assert_eq!(instance.generator.next_id().unwrap().worker(), moved);
}

pub async fn shutdown_releases_the_lease(infra: Arc<Infra>) {
    let instance = start(&infra).await;
    let worker = instance.generator.worker().unwrap();
    instance.stop.send(true).unwrap();
    instance.task.await.unwrap();
    assert!(!instance.generator.is_ready());

    let mut redis = storage::redisdb::connect(&infra.redis_url).await.unwrap();
    let holder: Option<String> = redis::cmd("GET")
        .arg(format!("worker:{worker}"))
        .query_async(&mut redis)
        .await
        .unwrap();
    assert_eq!(holder, None, "released lease is still held");
    let hwm: Option<u64> = redis::cmd("GET")
        .arg(format!("worker_hwm:{worker}"))
        .query_async(&mut redis)
        .await
        .unwrap();
    assert!(
        hwm.is_some(),
        "the high-water mark is recorded for the next holder"
    );
}

/// The production timings make the quarantine as long as the lease TTL. The
/// lease must be renewed during quarantine, or it expires just as the
/// quarantine ends and the instance never becomes ready. (Found by the
/// compose stack, which uses the real timings; these tests had used a zero
/// quarantine.)
pub async fn quarantine_keeps_the_lease_alive(infra: Arc<Infra>) {
    let started = tokio::time::Instant::now();
    let instance = start_with(
        &infra,
        LeaseTimings {
            ttl: Duration::from_secs(2),
            renew_every: Duration::from_millis(300),
            quarantine: Duration::from_secs(3),
            max_clock_skew: Duration::from_millis(500),
        },
    )
    .await;
    assert!(
        started.elapsed() >= Duration::from_secs(3),
        "IDs issued before the quarantine ended"
    );
    let worker = instance.generator.worker().unwrap();
    // Still the same worker ID: the lease was never lost.
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(instance.generator.worker(), Some(worker));
    assert!(instance.generator.is_ready());
}
