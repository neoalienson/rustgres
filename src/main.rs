use rustgres::protocol::Server;
use std::thread;

fn main() -> std::io::Result<()> {
    let addr = "127.0.0.1:5433";
    println!("🚀 RustGres v0.1.0 starting...");
    println!("📡 Listening on {}", addr);
    println!("✅ Ready for connections");
    println!("\nConnect with: psql -h 127.0.0.1 -p 5433 -U postgres -d testdb\n");
    
    let server = Server::bind(addr)?;
    
    loop {
        match server.accept() {
            Ok(mut conn) => {
                thread::spawn(move || {
                    if let Err(e) = conn.run() {
                        eprintln!("Connection error: {}", e);
                    }
                });
            }
            Err(e) => {
                eprintln!("Accept error: {}", e);
            }
        }
    }
}
