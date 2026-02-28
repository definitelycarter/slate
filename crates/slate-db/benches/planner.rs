use bson::Bson;
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use slate_db::bench::{Expression, Planner, Statement};
use slate_engine::{Catalog, Engine, EngineTransaction, KvEngine};
use slate_db::bench::Mutation;
use slate_query::{Sort, SortDirection};
use slate_store::MemoryStore;

// ── Setup ──────────────────────────────────────────────────────

/// Engine with "users" collection indexed on "status" and "age".
fn setup() -> KvEngine<MemoryStore> {
    let engine = KvEngine::new(MemoryStore::new());
    let mut txn = engine.begin(false).unwrap();
    txn.create_collection("users", &Default::default()).unwrap();
    txn.create_index("users", "status").unwrap();
    txn.create_index("users", "age").unwrap();
    txn.commit().unwrap();
    engine
}

// ── Helpers ────────────────────────────────────────────────────

fn find(predicate: Expression) -> Statement<'static> {
    Statement::Find {
        collection: "users",
        predicate,
        sort: vec![],
        skip: None,
        take: None,
        projection: None,
    }
}

fn find_with_sort(
    predicate: Expression,
    sort: Vec<Sort>,
    take: Option<usize>,
) -> Statement<'static> {
    Statement::Find {
        collection: "users",
        predicate,
        sort,
        skip: None,
        take,
        projection: None,
    }
}

fn find_with_projection(predicate: Expression, projection: Vec<String>) -> Statement<'static> {
    Statement::Find {
        collection: "users",
        predicate,
        sort: vec![],
        skip: None,
        take: None,
        projection: Some(projection),
    }
}

// ── Benchmarks ─────────────────────────────────────────────────

fn bench_plan_scan(c: &mut Criterion) {
    let engine = setup();
    c.bench_function("plan/scan", |b| {
        b.iter_batched(
            || {
                let txn = engine.begin(true).unwrap();
                let stmt = find(Expression::Exists("name".into(), true));
                (txn, stmt)
            },
            |(txn, stmt)| {
                let planner = Planner::new(|name| Ok(txn.collection(name)?));
                planner.plan(stmt).unwrap();
            },
            BatchSize::SmallInput,
        );
    });
}

fn bench_plan_index_eq(c: &mut Criterion) {
    let engine = setup();
    c.bench_function("plan/index_eq", |b| {
        b.iter_batched(
            || {
                let txn = engine.begin(true).unwrap();
                let stmt = find(Expression::Eq(
                    "status".into(),
                    Bson::String("active".into()),
                ));
                (txn, stmt)
            },
            |(txn, stmt)| {
                let planner = Planner::new(|name| Ok(txn.collection(name)?));
                planner.plan(stmt).unwrap();
            },
            BatchSize::SmallInput,
        );
    });
}

fn bench_plan_index_range(c: &mut Criterion) {
    let engine = setup();
    c.bench_function("plan/index_range", |b| {
        b.iter_batched(
            || {
                let txn = engine.begin(true).unwrap();
                let stmt = find(Expression::Gt("age".into(), Bson::Int32(21)));
                (txn, stmt)
            },
            |(txn, stmt)| {
                let planner = Planner::new(|name| Ok(txn.collection(name)?));
                planner.plan(stmt).unwrap();
            },
            BatchSize::SmallInput,
        );
    });
}

fn bench_plan_and_eq_plus_range(c: &mut Criterion) {
    let engine = setup();
    c.bench_function("plan/and_eq_range", |b| {
        b.iter_batched(
            || {
                let txn = engine.begin(true).unwrap();
                let stmt = find(Expression::And(vec![
                    Expression::Eq("status".into(), Bson::String("active".into())),
                    Expression::Gt("age".into(), Bson::Int32(21)),
                ]));
                (txn, stmt)
            },
            |(txn, stmt)| {
                let planner = Planner::new(|name| Ok(txn.collection(name)?));
                planner.plan(stmt).unwrap();
            },
            BatchSize::SmallInput,
        );
    });
}

fn bench_plan_and_dual_range(c: &mut Criterion) {
    let engine = setup();
    c.bench_function("plan/and_dual_range", |b| {
        b.iter_batched(
            || {
                let txn = engine.begin(true).unwrap();
                let stmt = find(Expression::And(vec![
                    Expression::Gte("age".into(), Bson::Int32(18)),
                    Expression::Lt("age".into(), Bson::Int32(65)),
                ]));
                (txn, stmt)
            },
            |(txn, stmt)| {
                let planner = Planner::new(|name| Ok(txn.collection(name)?));
                planner.plan(stmt).unwrap();
            },
            BatchSize::SmallInput,
        );
    });
}

fn bench_plan_or_index_merge(c: &mut Criterion) {
    let engine = setup();
    c.bench_function("plan/or_index_merge", |b| {
        b.iter_batched(
            || {
                let txn = engine.begin(true).unwrap();
                let stmt = find(Expression::Or(vec![
                    Expression::Eq("status".into(), Bson::String("active".into())),
                    Expression::Eq("status".into(), Bson::String("pending".into())),
                ]));
                (txn, stmt)
            },
            |(txn, stmt)| {
                let planner = Planner::new(|name| Ok(txn.collection(name)?));
                planner.plan(stmt).unwrap();
            },
            BatchSize::SmallInput,
        );
    });
}

fn bench_plan_or_mixed_fields(c: &mut Criterion) {
    let engine = setup();
    c.bench_function("plan/or_mixed_fields", |b| {
        b.iter_batched(
            || {
                let txn = engine.begin(true).unwrap();
                let stmt = find(Expression::Or(vec![
                    Expression::Eq("status".into(), Bson::String("active".into())),
                    Expression::Eq("age".into(), Bson::Int32(30)),
                ]));
                (txn, stmt)
            },
            |(txn, stmt)| {
                let planner = Planner::new(|name| Ok(txn.collection(name)?));
                planner.plan(stmt).unwrap();
            },
            BatchSize::SmallInput,
        );
    });
}

fn bench_plan_sort_indexed(c: &mut Criterion) {
    let engine = setup();
    c.bench_function("plan/sort_indexed", |b| {
        b.iter_batched(
            || {
                let txn = engine.begin(true).unwrap();
                let stmt = find_with_sort(
                    Expression::Exists("name".into(), true),
                    vec![Sort {
                        field: "age".into(),
                        direction: SortDirection::Asc,
                    }],
                    Some(10),
                );
                (txn, stmt)
            },
            |(txn, stmt)| {
                let planner = Planner::new(|name| Ok(txn.collection(name)?));
                planner.plan(stmt).unwrap();
            },
            BatchSize::SmallInput,
        );
    });
}

fn bench_plan_covered_index(c: &mut Criterion) {
    let engine = setup();
    c.bench_function("plan/covered_index", |b| {
        b.iter_batched(
            || {
                let txn = engine.begin(true).unwrap();
                let stmt = find_with_projection(
                    Expression::Eq("status".into(), Bson::String("active".into())),
                    vec!["_id".into(), "status".into()],
                );
                (txn, stmt)
            },
            |(txn, stmt)| {
                let planner = Planner::new(|name| Ok(txn.collection(name)?));
                planner.plan(stmt).unwrap();
            },
            BatchSize::SmallInput,
        );
    });
}

fn bench_plan_distinct(c: &mut Criterion) {
    let engine = setup();
    c.bench_function("plan/distinct", |b| {
        b.iter_batched(
            || {
                let txn = engine.begin(true).unwrap();
                let stmt = Statement::Distinct {
                    collection: "users",
                    field: "status".into(),
                    predicate: Expression::Eq("age".into(), Bson::Int32(25)),
                    sort: Some(SortDirection::Asc),
                    skip: None,
                    take: Some(100),
                };
                (txn, stmt)
            },
            |(txn, stmt)| {
                let planner = Planner::new(|name| Ok(txn.collection(name)?));
                planner.plan(stmt).unwrap();
            },
            BatchSize::SmallInput,
        );
    });
}

fn bench_plan_insert(c: &mut Criterion) {
    let engine = setup();
    c.bench_function("plan/insert", |b| {
        b.iter_batched(
            || {
                let txn = engine.begin(true).unwrap();
                let doc = bson::rawdoc! { "_id": 1, "name": "Alice" };
                let stmt = Statement::Insert {
                    collection: "users",
                    docs: vec![doc],
                };
                (txn, stmt)
            },
            |(txn, stmt)| {
                let planner = Planner::new(|name| Ok(txn.collection(name)?));
                planner.plan(stmt).unwrap();
            },
            BatchSize::SmallInput,
        );
    });
}

fn bench_plan_update(c: &mut Criterion) {
    let engine = setup();
    c.bench_function("plan/update", |b| {
        b.iter_batched(
            || {
                let txn = engine.begin(true).unwrap();
                let stmt = Statement::Update {
                    collection: "users",
                    predicate: Expression::Eq("status".into(), Bson::String("active".into())),
                    mutation: Mutation { ops: vec![] },
                    limit: Some(100),
                };
                (txn, stmt)
            },
            |(txn, stmt)| {
                let planner = Planner::new(|name| Ok(txn.collection(name)?));
                planner.plan(stmt).unwrap();
            },
            BatchSize::SmallInput,
        );
    });
}

fn bench_plan_delete(c: &mut Criterion) {
    let engine = setup();
    c.bench_function("plan/delete", |b| {
        b.iter_batched(
            || {
                let txn = engine.begin(true).unwrap();
                let stmt = Statement::Delete {
                    collection: "users",
                    predicate: Expression::Eq("status".into(), Bson::String("inactive".into())),
                    limit: None,
                };
                (txn, stmt)
            },
            |(txn, stmt)| {
                let planner = Planner::new(|name| Ok(txn.collection(name)?));
                planner.plan(stmt).unwrap();
            },
            BatchSize::SmallInput,
        );
    });
}

fn bench_plan_complex_and_or(c: &mut Criterion) {
    let engine = setup();
    c.bench_function("plan/complex_and_or", |b| {
        b.iter_batched(
            || {
                let txn = engine.begin(true).unwrap();
                let stmt = find(Expression::And(vec![
                    Expression::Or(vec![
                        Expression::Eq("status".into(), Bson::String("active".into())),
                        Expression::Eq("status".into(), Bson::String("pending".into())),
                    ]),
                    Expression::Gte("age".into(), Bson::Int32(18)),
                    Expression::Exists("email".into(), true),
                ]));
                (txn, stmt)
            },
            |(txn, stmt)| {
                let planner = Planner::new(|name| Ok(txn.collection(name)?));
                planner.plan(stmt).unwrap();
            },
            BatchSize::SmallInput,
        );
    });
}

criterion_group!(
    benches,
    bench_plan_scan,
    bench_plan_index_eq,
    bench_plan_index_range,
    bench_plan_and_eq_plus_range,
    bench_plan_and_dual_range,
    bench_plan_or_index_merge,
    bench_plan_or_mixed_fields,
    bench_plan_sort_indexed,
    bench_plan_covered_index,
    bench_plan_distinct,
    bench_plan_insert,
    bench_plan_update,
    bench_plan_delete,
    bench_plan_complex_and_or,
);
criterion_main!(benches);
