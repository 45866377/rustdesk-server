// Custom: minimal loopback-only status API for hbbs.
// Serves GET /api/health and GET /api/online, returning an in-memory snapshot
// of registered peers so a local admin page can display which clients are online.
// Binds to 127.0.0.1 only -- never exposed to the public network.
use crate::peer::PeerMap;
use hbb_common::{
    log,
    tokio::{
        self,
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
    },
};
use serde::Serialize;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

// Client heartbeat (RegisterPeer) keeps last_reg_time fresh; after this long
// without a heartbeat the peer is considered offline. Same value hbbs uses
// internally (REG_TIMEOUT = 30s).
const REG_TIMEOUT_MS: u64 = 30_000;

#[derive(Serialize)]
struct PeerRow {
    id: String,
    ip: String,
    last_seen_ms: u64,
    registered: bool,
}

pub(crate) async fn start(pm: PeerMap, port: u16) {
    match TcpListener::bind(("127.0.0.1", port)).await {
        Ok(listener) => {
            log::info!("Status API listening on 127.0.0.1:{}", port);
            tokio::spawn(async move {
                serve(listener, pm).await;
            });
        }
        Err(err) => log::error!("Status API failed to bind 127.0.0.1:{}: {}", port, err),
    }
}

async fn serve(listener: TcpListener, pm: PeerMap) {
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let pm = pm.clone();
                tokio::spawn(async move {
                    handle(stream, pm).await;
                });
            }
            Err(err) => {
                log::error!("Status API accept failed: {}", err);
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
}

async fn handle(mut stream: TcpStream, pm: PeerMap) {
    let mut buf = [0u8; 2048];
    let n = match stream.read(&mut buf).await {
        Ok(n) => n,
        Err(_) => return,
    };
    if n == 0 {
        return;
    }
    let req = String::from_utf8_lossy(&buf[..n]);
    let path = req
        .split_whitespace()
        .nth(1)
        .unwrap_or("/")
        .split('?')
        .next()
        .unwrap_or("/")
        .to_owned();
    let (status, body) = match path.as_str() {
        "/api/online" => ("200 OK", build_online(&pm).await),
        "/api/health" => ("200 OK", "{\"status\":\"ok\"}".to_owned()),
        _ => ("404 Not Found", "{\"error\":\"not found\"}".to_owned()),
    };
    let resp = format!(
        "HTTP/1.1 {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        status,
        body.len(),
        body
    );
    if stream.write_all(resp.as_bytes()).await.is_ok() {
        let _ = stream.flush().await;
    }
}

async fn build_online(pm: &PeerMap) -> String {
    let mut rows: Vec<PeerRow> = Vec::new();
    let mut registered_total = 0usize;
    for (id, lp) in pm.ids_and_peers().await {
        let p = lp.read().await;
        // Peers restored from db without a confirmed pk are not real clients.
        let registered = !p.pk.is_empty();
        if registered {
            registered_total += 1;
        }
        let last_seen_ms = p.last_reg_time.elapsed().as_millis() as u64;
        if registered && last_seen_ms < REG_TIMEOUT_MS {
            rows.push(PeerRow {
                id,
                ip: p.info.ip.clone(),
                last_seen_ms,
                registered,
            });
        }
    }
    rows.sort_by(|a, b| a.id.cmp(&b.id));
    let now_ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    serde_json::json!({
        "online_count": rows.len(),
        "registered_total": registered_total,
        "now_ts": now_ts,
        "online": rows,
    })
    .to_string()
}
