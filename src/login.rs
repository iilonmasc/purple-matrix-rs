//! Signing in with an access token: the client with its stores, `whoami` to prove the token and
//! learn whose it is, then the SDK's `restore_session` with what `whoami` said.
//!
//! The order is the SDK's doing: `restore_session` wants the user id and the device id up front,
//! because the crypto store is keyed by them, and only `whoami` knows them for a bare token. So
//! `whoami` goes out before there is a session, through the client's own HTTP client with the
//! token on the request, and the session is restored from its answer.
//!
//! The token itself lives in the client's memory and nowhere else: the SDK's stores hold the
//! session's user and device, the crypto account and the room state, never the access token.

use std::borrow::Cow;
use std::time::Duration;

use matrix_sdk::authentication::matrix::MatrixSession;
use matrix_sdk::ruma::api::client::account::whoami;
use matrix_sdk::ruma::api::error::{ErrorKind, FromHttpResponseError};
use matrix_sdk::ruma::api::{
    IncomingResponseExt, MatrixVersion, OutgoingRequestExt, SupportedVersions,
    auth_scheme::SendAccessToken,
};
use matrix_sdk::ruma::exports::http;
use matrix_sdk::{Client, SessionMeta, SessionTokens, reqwest};

/// How long `whoami` may take; a libpurple user is waiting on it.
const WHOAMI_TIMEOUT: Duration = Duration::from_secs(30);

/// Why libpurple is told the connection failed. The discriminants are libpurple's
/// `PurpleConnectionError` values, so `c/prpl.c` passes them through as they are.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorReason {
    /// `PURPLE_CONNECTION_ERROR_NETWORK_ERROR`: the homeserver was not reached.
    Network = 0,
    /// `PURPLE_CONNECTION_ERROR_AUTHENTICATION_FAILED`: the homeserver rejected the token.
    AuthenticationFailed = 2,
    /// `PURPLE_CONNECTION_ERROR_OTHER_ERROR`: everything else, with the SDK's message.
    Other = 16,
}

/// What went wrong signing in; the message is what libpurple shows.
#[derive(Debug, thiserror::Error)]
pub enum LoginError {
    #[error("no access token: put it in the password field")]
    NoToken,
    #[error("the homeserver was not reached: {0}")]
    Network(String),
    #[error("the homeserver rejected the access token: {0}")]
    Rejected(String),
    #[error("the access token belongs to no device, and end-to-end encryption needs one")]
    NoDevice,
    #[error("{0}")]
    Other(String),
}

impl LoginError {
    /// The libpurple reason this error maps to.
    pub fn reason(&self) -> ErrorReason {
        match self {
            Self::Network(_) => ErrorReason::Network,
            Self::NoToken | Self::Rejected(_) | Self::NoDevice => ErrorReason::AuthenticationFailed,
            Self::Other(_) => ErrorReason::Other,
        }
    }
}

/// Build the client for `homeserver` with its SQLite stores under `store_dir`, prove `token`
/// with `whoami`, restore the session it belongs to, and return the client with the user id the
/// token is for. Must run on the runtime that will drop the client.
pub async fn sign_in(
    homeserver: &str,
    store_dir: &str,
    token: &str,
) -> Result<(Client, String), LoginError> {
    if token.is_empty() {
        return Err(LoginError::NoToken);
    }
    let client = Client::builder()
        .homeserver_url(homeserver)
        .sqlite_store(store_dir, None)
        .build()
        .await
        .map_err(|e| LoginError::Other(format!("client build failed: {e}")))?;
    let identity = whoami(&client, token).await?;
    let device_id = identity.device_id.ok_or(LoginError::NoDevice)?;
    let user_id = identity.user_id.to_string();
    client
        .restore_session(MatrixSession {
            meta: SessionMeta {
                user_id: identity.user_id,
                device_id,
            },
            tokens: SessionTokens {
                access_token: token.to_owned(),
                refresh_token: None,
            },
        })
        .await
        .map_err(|e| LoginError::Other(format!("session restore failed: {e}")))?;
    Ok((client, user_id))
}

/// `GET /account/whoami` with `token`, before the client has a session: ruma builds the request
/// and reads the answer, the client's own HTTP client carries it.
async fn whoami(client: &Client, token: &str) -> Result<whoami::v3::Response, LoginError> {
    let versions = SupportedVersions {
        versions: [MatrixVersion::V1_1].into(),
        features: Default::default(),
    };
    let request = whoami::v3::Request::new()
        .try_into_http_request::<Vec<u8>>(
            client.homeserver().as_str(),
            SendAccessToken::Always(token),
            Cow::Borrowed(&versions),
        )
        .map_err(|e| LoginError::Other(format!("whoami request: {e}")))?;
    let mut request = reqwest::Request::try_from(request)
        .map_err(|e| LoginError::Other(format!("whoami request: {e}")))?;
    *request.timeout_mut() = Some(WHOAMI_TIMEOUT);
    let response = client
        .http_client()
        .execute(request)
        .await
        .map_err(|e| LoginError::Network(e.to_string()))?;
    let status = response.status();
    let body = response
        .bytes()
        .await
        .map_err(|e| LoginError::Network(e.to_string()))?;
    let mut http_response = http::Response::new(body.as_ref());
    *http_response.status_mut() = status;
    whoami::v3::Response::try_from_http_response(http_response).map_err(|e| match e {
        FromHttpResponseError::Server(server) => {
            let rejected = server.status_code == http::StatusCode::UNAUTHORIZED
                || matches!(
                    server.error_kind(),
                    Some(ErrorKind::UnknownToken { .. } | ErrorKind::Forbidden)
                );
            if rejected {
                LoginError::Rejected(server.to_string())
            } else {
                LoginError::Other(format!("whoami failed: {server}"))
            }
        }
        other => LoginError::Other(format!("whoami failed: {other}")),
    })
}
