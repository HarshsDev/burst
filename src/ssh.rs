use ssh2::Session;
use std::net::TcpStream;

pub(crate) struct Session {
    ssh: ssh2::Session,
    stream: TcpStream,
}

impl Session {
    pub fn connect<A: net::ToSocketAddrs>(addr: A) -> io::Result<ssh2::Session> {
        let tcp = TcpStream::connect(addr).unwrap();
        let mut sess = ssh2::Session::new().unwrap();
        sess.handshake(&tcp).unwrap();

        sess.userauth_agent()
    }
}
