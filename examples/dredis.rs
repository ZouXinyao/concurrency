use std::net::SocketAddr;

use anyhow::Result;
use tokio::io::{AsyncWriteExt, ErrorKind};
use tokio::net::TcpListener;
use tracing::{info, warn};

const BUFFER_SIZE: usize = 4096; // 1MB

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let addr = "0.0.0.0:6379";
    let listener = TcpListener::bind(addr).await?;
    info!("Dredis: Listening on: {}", addr);

    loop {
        let (stream, raddr) = listener.accept().await?;
        info!("Dredis: Accepted connection from: {}", raddr);
        tokio::spawn(async move {
            let _ = process_redis_conn(stream, raddr).await;
        });
    }
    // Ok(())
}

async fn process_redis_conn(mut stream: tokio::net::TcpStream, addr: SocketAddr) -> Result<()> {
    loop {
        stream.readable().await?;
        let mut buf = Vec::with_capacity(BUFFER_SIZE);

        match stream.try_read_buf(&mut buf) {
            Ok(0) => {
                info!("Dredis: Connection closed by client");
                break;
            }
            Ok(n) => {
                info!("Dredis: Received {} bytes", n);
                let line = String::from_utf8_lossy(&buf);
                info!("Dredis: Received: {:?}", line);
                stream.write_all(b"+Ok\r\n").await?;
            }
            Err(ref e) if e.kind() == ErrorKind::WouldBlock => {
                continue;
            }
            Err(e) => {
                return Err(e.into());
            }
        }
    }
    warn!("Dredis: Connection closed {}", addr);
    Ok(())
}
