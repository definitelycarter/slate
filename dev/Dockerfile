FROM rust:1.92-slim AS chef
WORKDIR /app
RUN apt-get update && apt-get install -y clang libclang-dev && rm -rf /var/lib/apt/lists/*
RUN cargo install cargo-chef

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
COPY . .
RUN cargo build --release -p slate-server -p slate-server-init -p slate-api -p slate-operator -p slate-lists-knative

FROM debian:trixie-slim AS server
RUN apt-get update && apt-get install -y libgcc-s1 && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/slate-server /usr/local/bin/slate-server
CMD ["slate-server"]

FROM debian:trixie-slim AS api
RUN apt-get update && apt-get install -y libgcc-s1 && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/slate-api /usr/local/bin/slate-api
CMD ["slate-api"]

FROM debian:trixie-slim AS operator
RUN apt-get update && apt-get install -y libgcc-s1 && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/slate-operator /usr/local/bin/slate-operator
CMD ["slate-operator"]

FROM debian:trixie-slim AS server-init
RUN apt-get update && apt-get install -y libgcc-s1 && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/slate-server-init /usr/local/bin/slate-server-init
CMD ["slate-server-init"]

FROM debian:trixie-slim AS lists
RUN apt-get update && apt-get install -y libgcc-s1 && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/slate-lists-knative /usr/local/bin/slate-lists-knative
CMD ["slate-lists-knative"]
