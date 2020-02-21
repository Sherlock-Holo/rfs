fn main() {
    tonic_build::compile_protos("proto/protocol.proto").unwrap();
}