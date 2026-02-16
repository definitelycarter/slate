FROM rust:1.92-slim AS builder
WORKDIR /app
RUN apt-get update && apt-get install -y clang libclang-dev && rm -rf /var/lib/apt/lists/*
COPY . .
RUN cargo build --release -p slate-server -p slate-api

FROM debian:bookworm-slim AS server
RUN apt-get update && apt-get install -y libgcc-s1 && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/slate-server /usr/local/bin/slate-server
CMD ["slate-server"]

FROM debian:bookworm-slim AS api
RUN apt-get update && apt-get install -y libgcc-s1 && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/slate-api /usr/local/bin/slate-api
CMD ["slate-api"]
