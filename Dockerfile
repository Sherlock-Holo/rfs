FROM clux/muslrust:nightly as build-client

COPY . /volume

WORKDIR /volume/rfs-client

RUN rustup component add rustfmt

RUN cargo build --release

FROM alpine:latest as client

COPY --from=build-client /volume/target/x86_64-unknown-linux-musl/release/rfs-client /rfs-client

ENTRYPOINT [ "/rfs-client", "-c" ]

FROM clux/muslrust:nightly as build-server

COPY . /volume

# build server
WORKDIR /volume/rfs-server

RUN rustup component add rustfmt

RUN cargo build --release

FROM alpine:latest as server

COPY --from=build-server /volume/target/x86_64-unknown-linux-musl/release/rfs-server /rfs-server

ENTRYPOINT [ "/rfs-server", "-c" ]
