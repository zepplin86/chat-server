// 빌드할 때 한 번 실행되는 스크립트.
// proto/chat.proto 를 읽어 gRPC 서버/클라이언트용 Rust 코드를 자동으로 만들어 준다.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("../../proto/chat.proto")?;
    Ok(())
}
