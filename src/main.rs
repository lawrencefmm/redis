mod redisdb;

use redisdb::Redisdb;
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{Shutdown, SocketAddr};
use std::str;
use std::time::Duration;

use mio::net::{TcpListener, TcpStream};
use mio::{Events, Interest, Poll, Token};

struct Conn {
    pub stream: TcpStream,
    pub want_read: bool,
    pub want_write: bool,
    pub want_close: bool,
    pub incoming_buf: Vec<u8>,
    pub outgoing_buf: Vec<u8>,
}

impl Conn {
    fn new(stream: TcpStream) -> Conn {
        Conn {
            stream: stream,
            want_read: true,
            want_write: false,
            want_close: false,
            incoming_buf: Vec::new(),
            outgoing_buf: Vec::new(),
        }
    }
}

fn try_one_req(conn: &mut Conn, redis: &mut Redisdb) -> bool {
    if conn.incoming_buf.len() < 4 {
        return false;
    }

    let msg_size = u32::from_be_bytes(conn.incoming_buf[0..4].try_into().unwrap()) as usize;

    if 4 + msg_size > conn.incoming_buf.len() {
        return false;
    }

    if msg_size <= 0 {
        conn.want_close = true;
        return false;
    }

    let msg = &conn.incoming_buf[4..];

    let key_size = u32::from_be_bytes(msg[0..4].try_into().unwrap()) as usize;
    println!("key_size: {key_size}");
    let key_bytes = &msg[4..(4 + key_size)];
    let key = str::from_utf8(key_bytes).unwrap();

    let data_size =
        u32::from_be_bytes(msg[(4 + key_size)..(8 + key_size)].try_into().unwrap()) as usize;

    let data_bytes = &msg[(8 + key_size)..(8 + key_size + data_size)];
    let mut data: Vec<u8> = Vec::new();

    data.extend_from_slice(data_bytes);

    match redis.insert(&key, data) {
        false => {
            conn.outgoing_buf.extend_from_slice(&u32::to_be_bytes(20));
            conn.outgoing_buf
                .extend_from_slice("key created in redis".as_bytes());
        }
        true => {
            conn.outgoing_buf.extend_from_slice(&u32::to_be_bytes(20));

            conn.outgoing_buf
                .extend_from_slice("key updated in redis".as_bytes());
        }
    }

    let ans = redis.get(&key).unwrap();
    print!("key: {key}, value: ");
    for (_i, &val) in ans.iter().enumerate() {
        print!("{val} ");
    }
    println!("");

    conn.incoming_buf.drain(..(4 + msg_size));
    return true;
}

fn handle_read(conn: &mut Conn, redis: &mut Redisdb) {
    let mut buf: [u8; 65536] = [0; 65536];
    assert!(conn.want_read);

    match conn.stream.read(&mut buf) {
        Ok(0) => {
            conn.want_close = true;
            return;
        }
        Ok(n) => {
            conn.incoming_buf.extend_from_slice(&buf[..n]);
            while try_one_req(conn, redis) {}
            if conn.outgoing_buf.len() > 0 {
                conn.want_read = false;
                conn.want_write = true;
            }
        }
        Err(e) => {
            eprintln!("Error reading {e}");
            conn.want_close = true;
            return;
        }
    }
}

fn handle_write(conn: &mut Conn) {
    assert!(conn.outgoing_buf.len() > 0);
    assert!(conn.want_write);

    match conn.stream.write(&conn.outgoing_buf) {
        Ok(n) => {
            println!("wrote {n} bytes");
            conn.outgoing_buf.drain(..n);
            if conn.outgoing_buf.len() == 0 {
                conn.want_read = true;
                conn.want_write = false;
            }
        }
        Err(e) => {
            eprintln!("error writing {e}");
            conn.want_close = true;
        }
    }
}

fn main() -> std::io::Result<()> {
    let mut redis = Redisdb::new();

    let addr: SocketAddr = "0.0.0.0:1234".parse().unwrap();
    let mut listener = TcpListener::bind(addr)?;

    let mut poll = Poll::new()?;
    let mut events = Events::with_capacity(20);

    let mut token_id = 1;
    let mut connections: HashMap<Token, Conn> = HashMap::new();

    poll.registry()
        .register(&mut listener, Token(0), Interest::READABLE)?;

    loop {
        poll.poll(&mut events, Some(Duration::from_millis(200)))?;

        for event in &events {
            if event.token() == Token(0) {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        poll.registry().register(
                            &mut stream,
                            Token(token_id),
                            Interest::READABLE | Interest::WRITABLE,
                        )?;
                        let conn = Conn::new(stream);
                        connections.insert(Token(token_id), conn);

                        token_id += 1;
                    }
                    Err(e) => {
                        if e.kind() != std::io::ErrorKind::WouldBlock {
                            return Err(e);
                        }
                    }
                }
            } else {
                let conn = connections.get_mut(&event.token()).unwrap();
                if event.is_readable() && conn.want_read {
                    handle_read(conn, &mut redis);
                }

                if event.is_writable() && conn.want_write {
                    handle_write(conn);
                }

                if conn.want_close {
                    poll.registry().deregister(&mut conn.stream)?; 
                    conn.stream.shutdown(Shutdown::Both)?;
                    connections.remove(&event.token());
                }
            }
        }
    }
}
