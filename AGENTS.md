## Development

### Building

```bash
cargo build
```

### Testing

```bash
cargo test
```

### Formatting

```bash
cargo fmt
```

## Guidelines

- Follow Rust idioms and best practices
- Use `cargo fmt` before committing
- Ensure all tests pass with `cargo test`
- Add tests for new functionality
- Do not automatically commit or push to this repository - wait for explicit user approval
- Avoid `clone()` in production code - provide justification if proposing it (acceptable in tests)
- Avoid `unwrap()`, `expect()`, and other panic-prone error handling in production code (acceptable in tests)
