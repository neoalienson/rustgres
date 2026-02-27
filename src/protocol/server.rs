use super::connection::Connection;
use std::net::{TcpListener, TcpStream};
use std::io;

pub struct Server {
    listener: TcpListener,
}

impl Server {
    pub fn bind(addr: &str) -> io::Result<Self> {
        let listener = TcpListener::bind(addr)?;
        Ok(Self { listener })
    }

    pub fn accept(&self) -> io::Result<Connection<TcpStream>> {
        let (stream, _) = self.listener.accept()?;
        Ok(Connection::new(stream))
    }

    pub fn local_addr(&self) -> io::Result<std::net::SocketAddr> {
        self.listener.local_addr()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_server_bind() {
        let server = Server::bind("127.0.0.1:0").unwrap();
        assert!(server.local_addr().is_ok());
    }
}
