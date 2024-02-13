FROM rust:1.76.0 AS builder
WORKDIR /usr/src/app
COPY Cargo.toml Cargo.lock ./
COPY .env .
RUN mkdir src && echo "fn main() {}" > src/main.rs
RUN cargo build --release
COPY src ./src
RUN cargo install --path .

FROM debian:bookworm-slim
WORKDIR /usr/src/app
COPY --from=builder /usr/local/cargo/bin/api-yt .
EXPOSE 80
CMD ["./api-yt"]
