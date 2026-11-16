//! 게이트웨이가 부르는 gRPC 입구.
//! 여기서는 요청을 풀어 핵심 로직(service.rs)에 넘기고, 결과를 응답 모양으로 싸 주기만 한다.

use std::sync::Arc;

use common::pb::chat_server::Chat;
use common::pb::{
    CheckJoinRequest, CheckJoinResponse, FetchMessagesRequest, FetchMessagesResponse,
    SendMessageRequest, SendMessageResponse,
};
use tonic::{Request, Response, Status};

use crate::service::Service;

/// gRPC 서버 본체. 안에 공유되는 핵심 로직을 들고 있다.
pub struct ChatGrpc(pub Arc<Service>);

#[tonic::async_trait]
impl Chat for ChatGrpc {
    /// 메시지 전송
    async fn send_message(
        &self,
        request: Request<SendMessageRequest>,
    ) -> Result<Response<SendMessageResponse>, Status> {
        // 요청 내용을 꺼낸다.
        let req = request.into_inner();
        // 핵심 로직 호출. 오류는 `?` 에서 gRPC 상태 코드로 자동 변환된다.
        let sent = self
            .0
            .send(
                req.user_id,
                &req.user_name,
                req.room_id,
                &req.client_msg_id,
                &req.body,
            )
            .await?;
        // 고유 번호, 순번, 중복 여부를 돌려준다.
        Ok(Response::new(SendMessageResponse {
            message_id: sent.message_id,
            seq: sent.seq,
            duplicate: sent.duplicate,
        }))
    }

    /// 방 입장 가능 여부 확인. 게이트웨이가 방을 구독하기 전에 부른다.
    async fn check_join(
        &self,
        request: Request<CheckJoinRequest>,
    ) -> Result<Response<CheckJoinResponse>, Status> {
        let req = request.into_inner();
        // 권한을 확인하고 방 종류를 얻는다. 권한이 없으면 여기서 오류로 끝난다.
        let room_type = self.0.room_access(req.user_id, req.room_id).await?;
        // 앱이 어디까지 받아야 하는지 알 수 있게 최신 순번도 같이 준다.
        let latest_seq = self.0.latest_seq(req.room_id).await?;
        Ok(Response::new(CheckJoinResponse {
            room_type: room_type.as_str().to_string(),
            latest_seq,
        }))
    }

    /// 지난 메시지 조회 (빠진 메시지 채우기, 재접속 후 따라잡기)
    async fn fetch_messages(
        &self,
        request: Request<FetchMessagesRequest>,
    ) -> Result<Response<FetchMessagesResponse>, Status> {
        let req = request.into_inner();
        let fetched = self
            .0
            .fetch(req.user_id, req.room_id, req.after_seq, req.limit)
            .await?;
        // 메시지는 이미 JSON 문자열이므로 그대로 실어 보낸다.
        Ok(Response::new(FetchMessagesResponse {
            messages: fetched.messages,
            latest_seq: fetched.latest_seq,
            has_more: fetched.has_more,
        }))
    }
}
