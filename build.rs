// Generates Rust gRPC stubs from the lymon-protos sibling repo.
// Assumes the workspace layout:
//   <parent>/lymon-agent/   (this crate)
//   <parent>/lymon-protos/  (sibling)

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_root = "../lymon-protos/proto";
    let proto_file = format!("{}/lymon/ingest/v1/ingest.proto", proto_root);

    tonic_build::configure()
        .build_server(false)
        .build_client(true)
        .compile_protos(&[&proto_file], &[proto_root])?;

    println!("cargo:rerun-if-changed={}", proto_file);
    Ok(())
}
