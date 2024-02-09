## docker file to build and run rust
FROM rust:1.75.0-slim-buster
WORKDIR /app
COPY src/ src/
COPY Cargo.toml .
COPY .env .
RUN cargo build --release
EXPOSE 80
ENTRYPOINT ["./target/release/api-yt"]
