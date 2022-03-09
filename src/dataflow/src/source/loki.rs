use std::{
    collections::HashMap,
    env,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::Context;
use async_trait::async_trait;
use mz_dataflow_types::SourceErrorDetails;
use mz_expr::SourceInstanceId;
use mz_repr::{Datum, Row};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::source::{SimpleSource, SourceError, Timestamper};

pub struct LokiSourceReader {
    source_id: SourceInstanceId,
    conn_info: LokiConnectionInfo,
    batch_window: Duration,
    query: String,
    client: reqwest::Client,
}

#[derive(Clone)]
pub struct LokiConnectionInfo {
    user: Option<String>,
    pw: Option<String>,
    endpoint: String,
}

impl LokiConnectionInfo {
    /// Loads connection information form the environment. Checks for `LOKI_ADDR`, `LOKI_USERNAME` and `LOKI_PASSWORD`.
    pub fn from_env() -> LokiConnectionInfo {
        let user = env::var("LOKI_USERNAME");
        let pw = env::var("LOKI_PASSWORD");
        let endpoint = env::var("LOKI_ADDR").unwrap_or_else(|_| "".to_string());
        info!("Connection info user={:?} pw={:?}", user, pw);
        LokiConnectionInfo {
            user: user.ok(),
            pw: pw.ok(),
            endpoint,
        }
    }

    pub fn with_user(mut self, user: Option<String>) -> LokiConnectionInfo {
        if user.is_some() {
            self.user = user;
        }
        self
    }

    pub fn with_password(mut self, password: Option<String>) -> LokiConnectionInfo {
        if password.is_some() {
            self.pw = password;
        }
        self
    }

    pub fn with_endpoint(mut self, address: Option<String>) -> LokiConnectionInfo {
        if let Some(address) = address {
            self.endpoint = address;
        }
        self
    }
}

impl LokiSourceReader {
    pub fn new(
        source_id: SourceInstanceId,
        mut conn_info: LokiConnectionInfo,
        query: String,
    ) -> LokiSourceReader {
        conn_info.endpoint = format!("{}/loki/api/v1/query_range", conn_info.endpoint);
        Self {
            source_id,
            conn_info,
            batch_window: Duration::from_secs(10),
            query,
            client: reqwest::Client::new(),
        }
    }

    #[tracing::instrument(skip(self), level = "trace")]
    async fn query(&self, start: u128, end: u128) -> Result<reqwest::Response, reqwest::Error> {
        let mut r = self.client.get(&self.conn_info.endpoint);
        if let Some(ref user) = self.conn_info.user {
            r = r.basic_auth(user, self.conn_info.pw.clone());
        };

        r.query(&[("query", &self.query)])
            .query(&[("start", format!("{}", start))])
            .query(&[("end", format!("{}", end))])
            .query(&[("direction", "forward")])
            .send()
            .await
    }

    #[tracing::instrument(skip(self), level = "trace")]
    async fn tick(&self) -> Result<Vec<String>, anyhow::Error> {
        let end = SystemTime::now();
        let start = end - self.batch_window;
        let response = self
            .query(
                start.duration_since(UNIX_EPOCH).unwrap().as_nanos(),
                end.duration_since(UNIX_EPOCH).unwrap().as_nanos(),
            )
            .await
            .context("Connection error")?
            .error_for_status()
            .context("HTTP error")?
            .bytes()
            .await
            .context("Download error")?;
        let result: QueryResult = serde_json::from_slice(&response).context("Deserialize error")?;
        let Data::Streams(streams) = result.data;

        #[derive(Debug, Serialize)]
        struct LokiRow<'a> {
            timestamp: &'a str,
            line: &'a str,
            labels: &'a HashMap<&'a str, &'a str>,
        }

        // TODO(bsull): we could get rid of this intermediate Vec if we handled the timestamp sending
        // in this function instead, but for now it's quite nice to be able to see the resulting JSON
        // in a test.
        let lines: Vec<String> = streams
            .iter()
            .flat_map(|s| {
                s.values.iter().map(|v| {
                    serde_json::to_string(&LokiRow {
                        timestamp: v.ts,
                        line: v.line,
                        labels: &s.labels,
                    })
                    .expect("Loki data should be valid JSON")
                })
            })
            .collect();
        Ok(lines)
    }
}

#[async_trait]
impl SimpleSource for LokiSourceReader {
    async fn start(mut self, timestamper: &Timestamper) -> Result<(), SourceError> {
        let mut interval = tokio::time::interval(self.batch_window);
        loop {
            interval.tick().await;
            match self.tick().await {
                Ok(lines) => {
                    let tx = timestamper.start_tx().await;
                    for line in lines {
                        tx.insert(Row::pack_slice(&[Datum::String(&line)]))
                            .await
                            .map_err(|e| {
                                SourceError::new(
                                    self.source_id,
                                    SourceErrorDetails::Persistence(e.to_string()),
                                )
                            })?;
                    }
                    Ok(())
                }
                Err(e) => {
                    warn!("Loki error: {}", e);
                    Err(SourceError::new(
                        self.source_id,
                        SourceErrorDetails::Persistence(e.to_string()),
                    ))
                }
            }?;
        }
    }
}

#[derive(Debug, Deserialize)]
struct QueryResult<'a> {
    status: &'a str,
    #[serde(borrow)]
    data: Data<'a>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "resultType", content = "result")]
enum Data<'a> {
    #[serde(borrow, rename = "streams")]
    Streams(Vec<Stream<'a>>),
}

#[derive(Debug, Deserialize)]
struct Stream<'a> {
    #[serde(borrow, rename = "stream")]
    labels: HashMap<&'a str, &'a str>,
    #[serde(borrow)]
    values: Vec<LogEntry<'a>>,
}

#[derive(Debug, Deserialize)]
struct LogEntry<'a> {
    #[serde(borrow)]
    ts: &'a str,
    #[serde(borrow)]
    line: &'a str,
}

#[cfg(test)]
mod test {

    use super::*;
    use mz_expr::GlobalId;

    #[tokio::test]
    async fn connect() {
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

        for _ in 0..5 {
            println!("{:?}", loki.tick().await);
        }
        // let fut = loki.new_stream().take(5);
        // fut.for_each(|data| async move {
        //     println!("{:?}", data);
        // })
        // .await;
    }
}
