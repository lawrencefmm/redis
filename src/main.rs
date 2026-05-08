mod redisdb;

use redisdb::Redisdb;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::str;

enum Status {
    Continue,
    End,
}

fn handle_client(
    redis: &mut Redisdb,
    buffer: &mut BufReader<&TcpStream>,
    mut stream: &TcpStream,
) -> std::io::Result<Status> {
    let mut msg_size_buf: [u8; 4] = [0; 4];
    if buffer.fill_buf()?.is_empty() {
        return Ok(Status::End);
    }
    buffer.read_exact(&mut msg_size_buf)?;

    let msg_size = u32::from_be_bytes(msg_size_buf) as usize;

    if msg_size <= 0 {
        return Ok(Status::End);
    }
    let mut msg_buf: Vec<u8> = vec![0; msg_size];

    buffer.read_exact(&mut msg_buf)?;

    let key_size = u32::from_be_bytes(msg_buf[0..4].try_into().unwrap()) as usize;
    println!("key_size: {key_size}");
    let key_bytes = &msg_buf[4..(4 + key_size)];
    let key = str::from_utf8(key_bytes).unwrap();

    let data_size =
        u32::from_be_bytes(msg_buf[(4 + key_size)..(8 + key_size)].try_into().unwrap()) as usize;
    let data_bytes = &msg_buf[(8 + key_size)..(8 + key_size + data_size)];
    let mut data: Vec<u8> = Vec::new();
    data.extend_from_slice(data_bytes);

    match redis.insert(&key, data) {
        false => {
            stream.write_all("key created in redis".as_bytes())?;
        }
        true => {
            stream.write_all("key updated in redis".as_bytes())?;
        }
    }
    let ans = redis.get(&key).unwrap();
    print!("key: {key}, value: ");
    for (_i, &val) in ans.iter().enumerate() {
        print!("{val} ");
    }
    println!("");

    Ok(Status::Continue)
}

fn main() -> std::io::Result<()> {
    let mut redis = Redisdb::new();

    let listener = TcpListener::bind("0.0.0.0:1234")?;

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("hey");
                let mut reader = BufReader::new(&stream);
                loop {
                    match handle_client(&mut redis, &mut reader, &stream) {
                        Ok(Status::Continue) => {
                            println!("oii thiagoooo");
                        }
                        Ok(Status::End) => {
                            println!("Ending reads");
                            break;
                        }
                        Err(e) => {
                            eprintln!("eita {e}");
                            break;
                        }
                    }
                }
            }
            Err(e) => {
                println!("Error in buffer! {e}");
                //break;
            }
        }
    }
    Ok(())
}
