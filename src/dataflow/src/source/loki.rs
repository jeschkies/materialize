use std::{
    borrow::Cow,
    collections::HashMap,
    env,
    io::Write,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::Context;
use async_trait::async_trait;
use base64::write::EncoderWriter as Base64Encoder;
use futures::StreamExt;
use mz_dataflow_types::SourceErrorDetails;
use mz_expr::SourceInstanceId;
use mz_ore::display::DisplayExt;
use mz_repr::{Datum, Row};
use serde::{Deserialize, Serialize};
use tokio::net::TcpStream;
use tokio_tungstenite::{
    connect_async, tungstenite::client::IntoClientRequest, MaybeTlsStream, WebSocketStream,
};
use tracing::warn;

use crate::source::{SimpleSource, SourceError, Timestamper};

/// A [`SimpleSource`] that reads from [Loki] over websockets using the [tail] endpoint.
///
/// [Loki]: https://grafana.com/docs/loki/latest/
/// [Loki]: https://grafana.com/docs/loki/latest/api/#get-lokiapiv1tail
pub struct LokiSourceReader {
    source_id: SourceInstanceId,
    conn_info: LokiConnectionInfo,
    query: String,
}

/// Loki connection information.
#[derive(Clone)]
pub struct LokiConnectionInfo {
    user: Option<String>,
    pw: Option<String>,
    endpoint: String,
}

impl LokiConnectionInfo {
    /// Loads connection information form the environment. Checks for `LOKI_ADDR`, `LOKI_USERNAME` and `LOKI_PASSWORD`.
    pub fn from_env() -> LokiConnectionInfo {
        let user = env::var("LOKI_USERNAME").ok();
        let pw = env::var("LOKI_PASSWORD").ok();
        let endpoint = env::var("LOKI_ADDR").unwrap_or_else(|_| "".to_string());
        LokiConnectionInfo { user, pw, endpoint }
    }

    /// Sets the username.
    pub fn with_user(mut self, user: Option<String>) -> LokiConnectionInfo {
        if user.is_some() {
            self.user = user;
        }
        self
    }

    /// Sets the password.
    pub fn with_password(mut self, password: Option<String>) -> LokiConnectionInfo {
        if password.is_some() {
            self.pw = password;
        }
        self
    }

    /// Sets the endpoint.
    pub fn with_endpoint(mut self, address: Option<String>) -> LokiConnectionInfo {
        if let Some(address) = address {
            self.endpoint = address;
        }
        self
    }
}

impl LokiSourceReader {
    /// Create a new `LokiSourceReader`.
    pub fn new(
        source_id: SourceInstanceId,
        mut conn_info: LokiConnectionInfo,
        query: String,
    ) -> LokiSourceReader {
        conn_info.endpoint = format!("{}/loki/api/v1/tail", conn_info.endpoint);
        Self {
            source_id,
            conn_info,
            query,
        }
    }

    async fn get_stream(
        &self,
    ) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>, anyhow::Error> {
        let start = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("Start must be after unix epoch")?
            .as_nanos();
        let mut url = url::Url::parse(&self.conn_info.endpoint).context("parsing Loki endpoint")?;
        url.set_scheme("wss")
            .map_err(|_| anyhow::anyhow!("error switching Loki endpoint to wss scheme"))?;
        url.query_pairs_mut()
            .clear()
            .append_pair("query", &self.query)
            .append_pair("limit", "5000")
            .append_pair("start", &start.to_string());
        let mut request = url.into_client_request().context("creating Loki request")?;
        if let Some(ref user) = self.conn_info.user {
            // Taken from `reqwest::RequestBuilder::basic_auth`
            let mut auth = b"Basic ".to_vec();
            {
                let mut encoder = Base64Encoder::new(&mut auth, base64::STANDARD);
                // The unwraps here are fine because Vec::write* is infallible.
                write!(encoder, "{user}:").unwrap();
                if let Some(ref password) = self.conn_info.pw {
                    write!(encoder, "{password}").unwrap();
                }
            }
            request
                .headers_mut()
                // The unwrap below is fine because we've just base64 encoded the user supplied input.
                .insert("Authorization", auth.try_into().unwrap());
        }
        let (stream, response) = connect_async(request)
            .await
            .context("connecting to Loki websocket")?;
        anyhow::ensure!(response.status().is_informational() || response.status().is_success());
        Ok(stream)
    }
}

#[async_trait]
impl SimpleSource for LokiSourceReader {
    async fn start(mut self, timestamper: &Timestamper) -> Result<(), SourceError> {
        'outer: loop {
            let mut stream = self.get_stream().await.map_err(|e| {
                SourceError::new(
                    self.source_id,
                    SourceErrorDetails::Initialization(e.to_string_alt()),
                )
            })?;
            'inner: while let Some(message) = stream.next().await {
                let message = match message {
                    Ok(m) => m.into_data(),
                    Err(error) => {
                        // We probably won't be able to continue with this stream; let's reconnect
                        // and start again.
                        warn!(%error, "Error in Loki stream. Attempting reconnect in 5 seconds");
                        tokio::time::sleep(Duration::from_secs(5)).await;
                        continue 'outer;
                    }
                };
                if message.is_empty() {
                    // Loki returns returns an empty message if there have been no logs
                    // since last tick, so we can just continue here.
                    continue 'inner;
                }
                let streams = match serde_json::from_slice(&message) {
                    Ok(TailResponse { streams }) => streams,
                    Err(error) => {
                        let response = String::from_utf8(message);
                        warn!(?response, %error, "Error deserializing Loki stream");
                        continue 'inner;
                    }
                };

                #[derive(Debug, Serialize)]
                struct LokiRow<'a> {
                    timestamp: &'a str,
                    line: &'a str,
                    labels: &'a HashMap<Cow<'a, str>, Cow<'a, str>>,
                }

                // TODO(bsull): we could get rid of this intermediate Vec if we handled the timestamp sending
                // in this function instead, but for now it's quite nice to be able to see the resulting JSON
                // in a test.
                let tx = timestamper.start_tx().await;
                for s in streams {
                    for v in s.values {
                        let row = serde_json::to_string(&LokiRow {
                            timestamp: v.ts,
                            line: &v.line,
                            labels: &s.labels,
                        })
                        .expect("Loki data should be valid JSON");
                        tx.insert(Row::pack_slice(&[Datum::String(&row)]))
                            .await
                            .map_err(|e| {
                                SourceError::new(
                                    self.source_id,
                                    SourceErrorDetails::Persistence(e.to_string_alt()),
                                )
                            })?;
                    }
                }
            }
        }
    }
}

#[derive(Debug, Deserialize)]
struct TailResponse<'a> {
    #[serde(borrow, rename = "streams")]
    streams: Vec<Stream<'a>>,
}

#[derive(Debug, Deserialize)]
struct Stream<'a> {
    #[serde(borrow, rename = "stream")]
    labels: HashMap<Cow<'a, str>, Cow<'a, str>>,
    #[serde(borrow)]
    values: Vec<LogEntry<'a>>,
}

#[derive(Debug, Deserialize)]
struct LogEntry<'a> {
    #[serde(borrow)]
    ts: &'a str,
    #[serde(borrow)]
    line: Cow<'a, str>,
}

#[cfg(test)]
mod test {

    use super::*;
    use futures::TryStreamExt;
    use mz_expr::GlobalId;

    #[tokio::test]
    async fn connect() -> anyhow::Result<()> {
        let user = "5442";
        let pw = "";
        let endpoint = "https://logs-prod-us-central1.grafana.net";
        let uid = SourceInstanceId {
            source_id: GlobalId::Explain,
            dataflow_id: 1,
        };

        let loki = LokiSourceReader::new(
            uid,
            LokiConnectionInfo {
                user: Some(user.to_string()),
                pw: Some(pw.to_string()),
                endpoint: endpoint.to_string(),
            },
            "{job=\"systemd-journal\"}".to_owned(),
        );

        loki.get_stream()
            .await?
            .take(5)
            .try_for_each(|data| async move {
                println!(
                    "{:?}",
                    serde_json::from_slice::<TailResponse>(&data.into_data()).unwrap()
                );
                Ok(())
            })
            .await?;
        Ok(())
        // let fut = loki.new_stream().take(5);
        // fut.for_each(|data| async move {
        //     println!("{:?}", data);
        // })
        // .await;
    }
}
