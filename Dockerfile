FROM rust:1.81 as builder
WORKDIR /usr/src/recsync-rs
COPY . .
RUN cargo install --path recceiver

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y libssl3 && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/local/cargo/bin/recceiver /usr/local/bin/recceiver
CMD ["recceiver"]
