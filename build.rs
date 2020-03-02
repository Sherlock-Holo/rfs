#[rustversion::not(nightly)]
fn main() {
    tonic_build::compile_protos("proto/protocol.proto").unwrap();
}

#[rustversion::nightly]
fn main() {
    println!("cargo:rustc-cfg=backtrace");

    tonic_build::compile_protos("proto/protocol.proto").unwrap();
}
