FROM clux/muslrust:nightly as build

COPY . /volume

# build client
WORKDIR /volume/rfs-client

RUN rustup component add rustfmt

RUN cargo build --release

# build server
WORKDIR /volume/rfs-server

RUN cargo build --release

FROM alpine:latest as client

COPY --from=build /volume/target/x86_64-unknown-linux-musl/rfs-client /rfs-client

ENTRYPOINT [ "/rfs-client", "-c" ]

FROM alpine:latest as server

COPY --from=build /volume/target/x86_64-unknown-linux-musl/rfs-server /rfs-server

ENTRYPOINT [ "/rfs-server", "-c" ]
