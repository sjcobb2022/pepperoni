use tokio::io::copy_bidirectional;
use tokio::net::{TcpListener, TcpStream};

use std::env;
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let listen_addr = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:8081".to_string());
    let server_addr = env::args()
        .nth(2)
        .unwrap_or_else(|| "127.0.0.1:8080".to_string());

    println!("Listening on: {listen_addr}");
    println!("Proxying to: {server_addr}");

    let listener = TcpListener::bind(listen_addr).await?;

    while let Ok((mut inbound, _)) = listener.accept().await {
        let server_addr = server_addr.clone();

        tokio::spawn(async move {
            let mut outbound = match TcpStream::connect(server_addr).await {
                Ok(outbound) => outbound,
                Err(e) => {
                    println!("Failed to connect; error={e}");
                    return;
                }
            };

            if let Err(e) = copy_bidirectional(&mut inbound, &mut outbound).await {
                println!("Failed to transfer; error={e}");
            }
        });
    }

    Ok(())
}
