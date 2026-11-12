//! Bounded WebSocket connections using Hex's proxy selection and platform TLS.
use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

use base64::Engine;
use tungstenite::{WebSocket, client::IntoClientRequest, stream::MaybeTlsStream};

pub(crate) type Socket = WebSocket<MaybeTlsStream<TcpStream>>;
const CONNECT: Duration = Duration::from_secs(10);

/// Connects with platform certificate verification, never logging credentials
/// or server error bodies. A short read timeout lets the owner poll cancellation.
pub(crate) fn connect(url: &str, key: &str) -> Result<Socket, String> {
    let parsed = url::Url::parse(url).map_err(|_| "Invalid streaming URL")?;
    let host = parsed.host_str().ok_or("Missing streaming host")?;
    let port = parsed
        .port_or_known_default()
        .ok_or("Missing streaming port")?;
    let https = url.replacen("wss:", "https:", 1);
    let proxy = super::proxy_for(&https);
    let (tcp_host, tcp_port) = proxy
        .as_ref()
        .map_or((host, port), |p| (p.host(), p.port()));
    let addresses = (tcp_host, tcp_port)
        .to_socket_addrs()
        .map_err(|_| "Couldn't look up the streaming server")?;
    let mut tcp = addresses
        .take(4)
        .find_map(|address| TcpStream::connect_timeout(&address, CONNECT).ok())
        .ok_or("Couldn't connect to the streaming server")?;
    tcp.set_read_timeout(Some(CONNECT))
        .map_err(|_| "Couldn't set streaming timeout")?;
    tcp.set_write_timeout(Some(CONNECT))
        .map_err(|_| "Couldn't set streaming timeout")?;
    if let Some(proxy) = proxy {
        // HTTPS and SOCKS proxies need their own transports. Never bypass a
        // configured proxy or send its credentials to the destination.
        if proxy.protocol() != ureq::ProxyProtocol::Http {
            return Err("Live transcription needs an HTTP CONNECT proxy on this network".into());
        }
        let auth = match (proxy.username(), proxy.password()) {
            (Some(user), password) => format!(
                "Proxy-Authorization: Basic {}\r\n",
                base64::engine::general_purpose::STANDARD
                    .encode(format!("{user}:{}", password.unwrap_or("")))
            ),
            _ => String::new(),
        };
        write!(
            tcp,
            "CONNECT {host}:{port} HTTP/1.1\r\nHost: {host}:{port}\r\n{auth}\r\n"
        )
        .map_err(|_| "The proxy connection failed")?;
        let mut response = Vec::new();
        while !response.ends_with(b"\r\n\r\n") && response.len() < 8192 {
            let mut byte = [0];
            tcp.read_exact(&mut byte)
                .map_err(|_| "The proxy connection failed")?;
            response.push(byte[0]);
        }
        let status = String::from_utf8_lossy(&response);
        if !status
            .lines()
            .next()
            .is_some_and(|line| line.split_whitespace().nth(1) == Some("200"))
        {
            return Err("The proxy refused the streaming connection".into());
        }
    }
    let tls = native_tls::TlsConnector::new()
        .map_err(|_| "Couldn't initialize platform TLS")?
        .connect(host, tcp)
        .map_err(|_| "The streaming server's secure connection failed")?;
    let mut request = url
        .into_client_request()
        .map_err(|_| "Invalid streaming request")?;
    let mut authorization = format!("Bearer {key}")
        .parse::<tungstenite::http::HeaderValue>()
        .map_err(|_| "Invalid gateway key")?;
    authorization.set_sensitive(true);
    request.headers_mut().insert("Authorization", authorization);
    request.headers_mut().insert(
        "Sec-WebSocket-Protocol",
        "ai-gateway-transcription.v1".parse().unwrap(),
    );
    let config = tungstenite::protocol::WebSocketConfig::default()
        .max_message_size(Some(256 * 1024))
        .max_frame_size(Some(256 * 1024));
    let (socket, _) = tungstenite::client::client_with_config(
        request,
        MaybeTlsStream::NativeTls(tls),
        Some(config),
    )
    .map_err(
        |_| "Vercel refused the streaming connection; check the gateway key and model access",
    )?;
    if let MaybeTlsStream::NativeTls(stream) = socket.get_ref() {
        stream
            .get_ref()
            .set_read_timeout(Some(Duration::from_millis(20)))
            .map_err(|_| "Couldn't set streaming timeout")?;
        stream
            .get_ref()
            .set_write_timeout(Some(Duration::from_secs(2)))
            .map_err(|_| "Couldn't set streaming timeout")?;
    }
    Ok(socket)
}
