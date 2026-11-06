// Task 13: Criterion throughput benchmarks (spec §Testing / Perf).
//
// Reports ops/sec + ns/op for PUT, GET, PUB+fanout(10) on the single
// thread ceiling. All benches drive `Core::apply` directly with
// `sync = false` (async commit; durability window <= 0.5ms group
// commit per spec §WAL).

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use realtime_backend::core::Core;
use realtime_backend::op::{ConnId, Op};

fn bench_put(c: &mut Criterion) {
    let mut core = Core::new_in_memory();
    let value = vec![1u8; 100];
    c.bench_function("put", |b| {
        b.iter(|| {
            let _ = core.apply(Op::Put {
                key: black_box("bench-k".to_string()),
                value: black_box(value.clone()),
                sync: false,
            });
        })
    });
}

fn bench_get(c: &mut Criterion) {
    let mut core = Core::new_in_memory();
    let _ = core.apply(Op::Put {
        key: "bench-k".to_string(),
        value: vec![7u8; 100],
        sync: false,
    });
    c.bench_function("get", |b| {
        b.iter(|| {
            let _ = core.apply(Op::Get {
                key: black_box("bench-k".to_string()),
            });
        })
    });
}

fn bench_pub_fanout10(c: &mut Criterion) {
    let mut core = Core::new_in_memory();
    // Pre-register 10 subscribers on the bench topic so the bench
    // exercises the fanout path (pub path iterates subscribed conns).
    let _ = core.apply(Op::Sub {
        topic: "bench".to_string(),
    });
    {
        let topic = core.topics.get_mut("bench").expect("bench topic");
        for i in 0..10u64 {
            topic.add_sub(ConnId(i)).expect("add sub");
        }
    }
    let mut group = c.benchmark_group("pub_fanout10");
    group.throughput(Throughput::Elements(10));
    group.bench_function("pub_fanout10", |b| {
        b.iter(|| {
            let _ = core.apply(Op::Pub {
                topic: black_box("bench".to_string()),
                value: black_box(vec![9u8; 100]),
                sync: false,
            });
            // Simulate delivering the published message to each of the
            // 10 subscribers (fanout loop over topic subs).
            let topic = core.topic("bench").expect("bench topic");
            for _ in topic.subs() {
                let _ = black_box(());
            }
        })
    });
    group.finish();
}

criterion_group!(benches, bench_put, bench_get, bench_pub_fanout10);
criterion_main!(benches);
