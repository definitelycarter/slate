//! v1-vs-v2 pipeline execution comparison.
//!
//! Both pipelines run the same logical query over the **same** seeded
//! `KvEngine` transaction. Plans are built in `iter_batched` setup (untimed),
//! so we measure *execution only* — excluding v1's planning and v2's request
//! translation, the fair apples-to-apples comparison.

use bson::{Bson, Document, RawDocumentBuf, doc};
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use slate_engine::{Catalog, DEFAULT_CF, Engine, EngineTransaction, KvEngine};
use slate_store::MemoryStore;

// v1 internals
use slate_db::bench::{Executor as V1Executor, Expression, Node as V1Node, Plan as V1Plan};
// v2
use slate_planner::{CollectionRef, Node as V2Node, Plan as V2Plan};
use slate_sql::ast::{BinOp, Literal, ScalarExpr};

const COLL: &str = "bench";
const N: usize = 10_000;

fn seed(n: usize) -> KvEngine<MemoryStore> {
    let engine = KvEngine::new(MemoryStore::new());
    {
        let txn = engine.begin(false).unwrap();
        txn.create_collection(DEFAULT_CF, COLL, &Default::default())
            .unwrap();
        txn.create_index(DEFAULT_CF, COLL, "age").unwrap();
        txn.commit().unwrap();
    }
    {
        let txn = engine.begin(false).unwrap();
        let handle = txn.collection(DEFAULT_CF, COLL).unwrap();
        for i in 0..n {
            let d: Document = doc! {
                "_id": format!("{i:06}"),
                "name": format!("user-{i}"),
                "age": (18 + (i % 60)) as i32,
                "status": if i % 2 == 0 { "active" } else { "inactive" },
            };
            txn.put(&handle, &RawDocumentBuf::try_from(&d).unwrap())
                .unwrap();
        }
        txn.commit().unwrap();
    }
    engine
}

fn v2_path(field: &str) -> ScalarExpr {
    ScalarExpr::Member {
        base: Box::new(ScalarExpr::Identifier("c".into())),
        field: field.into(),
    }
}

fn v2_coll() -> CollectionRef {
    CollectionRef {
        cf: DEFAULT_CF.into(),
        collection: COLL.into(),
    }
}

/// Full scan → return the whole document.
fn bench_scan(c: &mut Criterion) {
    let engine = seed(N);
    let txn = engine.begin(true).unwrap();
    let handle = txn.collection(DEFAULT_CF, COLL).unwrap();

    let mut group = c.benchmark_group("pipeline_scan");

    group.bench_function("v1", |b| {
        b.iter_batched(
            || {
                V1Plan::Find(V1Node::Scan {
                    collection: handle.clone(),
                })
            },
            |plan| V1Executor::new(&txn, None).execute(plan).unwrap().count(),
            BatchSize::SmallInput,
        );
    });

    group.bench_function("v2", |b| {
        b.iter_batched(
            || {
                V2Plan::Query(V2Node::Project {
                    expr: ScalarExpr::Identifier("c".into()),
                    source: Box::new(V2Node::Bind {
                        alias: "c".into(),
                        source: Box::new(V2Node::Scan {
                            collection: v2_coll(),
                        }),
                    }),
                })
            },
            |plan| {
                slate_executor::Executor::new(&txn)
                    .execute(plan)
                    .unwrap()
                    .count()
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

/// Filtered scan (`age > 50`) → return matching documents.
fn bench_filter(c: &mut Criterion) {
    let engine = seed(N);
    let txn = engine.begin(true).unwrap();
    let handle = txn.collection(DEFAULT_CF, COLL).unwrap();

    let mut group = c.benchmark_group("pipeline_filter");

    group.bench_function("v1", |b| {
        b.iter_batched(
            || {
                V1Plan::Find(V1Node::Filter {
                    predicate: Expression::Gt("age".into(), Bson::Int32(50)),
                    source: Box::new(V1Node::Scan {
                        collection: handle.clone(),
                    }),
                })
            },
            |plan| V1Executor::new(&txn, None).execute(plan).unwrap().count(),
            BatchSize::SmallInput,
        );
    });

    group.bench_function("v2", |b| {
        b.iter_batched(
            || {
                let filter = V2Node::Filter {
                    predicate: ScalarExpr::Binary {
                        op: BinOp::Gt,
                        lhs: Box::new(v2_path("age")),
                        rhs: Box::new(ScalarExpr::Literal(Literal::Int(50))),
                    },
                    source: Box::new(V2Node::Bind {
                        alias: "c".into(),
                        source: Box::new(V2Node::Scan {
                            collection: v2_coll(),
                        }),
                    }),
                };
                V2Plan::Query(V2Node::Project {
                    expr: ScalarExpr::Identifier("c".into()),
                    source: Box::new(filter),
                })
            },
            |plan| {
                slate_executor::Executor::new(&txn)
                    .execute(plan)
                    .unwrap()
                    .count()
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

criterion_group!(benches, bench_scan, bench_filter);
criterion_main!(benches);
