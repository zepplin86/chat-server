// 빌드하기 전에 먼저 도는 작은 프로그램. (build.rs 라는 이름이면 cargo 가 알아서 먼저 돌린다)
//
// proto/chat.proto 에 적은 약속을 읽어 Rust 코드로 만들어 둔다.
// 만들어진 코드는 src/lib.rs 에서 가져다 쓴다.
// 이 일에는 protoc 라는 프로그램이 필요하다. 없으면: brew install protobuf
fn main() {
    tonic_prost_build::compile_protos("proto/chat.proto")
        .expect("proto/chat.proto 를 Rust 코드로 만들지 못했습니다. protoc 가 깔려 있는지 확인하세요");
}
