//! Representative end-to-end benchmark: lex, parse, and execute a query that
//! exercises a join, a filter, an object projection, and an ordered limit.
//!
//! Kept intentionally minimal — the language surface is still evolving, so this
//! exists to catch gross regressions across the three stages, not to micro-
//! optimize. Add focused cases as the surface stabilizes.

use bson::{Bson, bson};
use criterion::{Criterion, criterion_group, criterion_main};
use slate_sql::{exec, lexer, parser};

const QUERY: &str = r#"SELECT VALUE { "who": c.name, "tag": t }
    FROM c JOIN t IN c.tags
    WHERE c.age > 21
    ORDER BY c.name ASC
    OFFSET 0 LIMIT 10"#;

fn sample_docs(n: usize) -> Vec<Bson> {
    (0..n)
        .map(|i| {
            bson!({
                "name": format!("person-{i}"),
                "age": (18 + (i % 50)) as i64,
                "tags": ["a", "b", "c"],
            })
        })
        .collect()
}

fn bench_sql(c: &mut Criterion) {
    c.bench_function("lex", |b| {
        b.iter(|| lexer::tokenize(QUERY).unwrap());
    });

    let tokens = lexer::tokenize(QUERY).unwrap();
    c.bench_function("parse", |b| {
        b.iter(|| parser::Parser::new(tokens.clone()).parse_query().unwrap());
    });

    let query = slate_sql::parse(QUERY).unwrap();
    let docs = sample_docs(1_000);
    c.bench_function("exec_1k", |b| {
        b.iter(|| exec::execute(&query, &docs).unwrap());
    });
}

criterion_group!(benches, bench_sql);
criterion_main!(benches);
