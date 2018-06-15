use super::redis::*;
use super::{Stream};

pub struct Sender {
    conn: Connection,
    stream_id: String,
}

impl Sender {
    pub fn connect(url: &str, stream_id: &str) -> Result<Self, RedisError> {
        let client = Client::open(url)?;
        let conn = client.get_connection()?;
        Ok(Self::new(conn, stream_id))
    }

    pub fn new(conn: Connection, stream_id: &str) -> Self {
        let stream_id = String::from(stream_id);
        Sender { conn, stream_id}
    }

    pub fn send<K: AsRef<[u8]>, V: AsRef<[u8]>>(&mut self, k: K, v: V) -> Result<(), RedisError> {
        Ok(cmd("XADD")
            .arg(&self.stream_id)
            .arg("*")
            .arg(k.as_ref())
            .arg(v.as_ref())
            .query(&self.conn)?)
    }
}

pub struct Receiver {
    conn: Connection,
    stream_id: String,
    entry_id: String,
}

impl Receiver {
    pub fn connect(url: &str, stream_id: &str, entry_id: &str) -> Result<Self, RedisError> {
        let client = Client::open(url)?;
        let conn = client.get_connection()?;
        Ok(Self::new(conn, stream_id, entry_id))
    }

    pub fn new(conn: Connection, stream_id: &str, entry_id: &str) -> Self {
        let stream_id = String::from(stream_id);
        let entry_id = String::from(entry_id);
        Receiver { conn, stream_id, entry_id}
    }

    pub fn recv(&mut self) -> Result<(Vec<u8>, Vec<u8>), RedisError> {
        let streams: Vec<Stream> = cmd("XREAD")
            .arg("COUNT")
            .arg(1)
            .arg("BLOCK")
            .arg(0)
            .arg("STREAMS")
            .arg(&self.stream_id)
            .arg(&self.entry_id)
            .query(&self.conn)?;
        for stream in streams {
            let stream_id: String = stream.id()?;
            assert!(stream_id == self.stream_id);
            for entry in stream.entries() {
                self.stream_id = entry.id()?;
                let mut key_values = entry.key_values();
                let k: Vec<u8> = from_redis_value(key_values.next().unwrap())?;
                let v: Vec<u8> = from_redis_value(key_values.next().unwrap())?;
                return Ok((k, v))
            }            
            panic!("No entries found");
        }
        panic!("No streams found");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_server() {
        let client = Client::open("redis://127.0.0.1/").unwrap();
        let con = client.get_connection().unwrap();        
        let stream_id = "_test_def";
        let _: () = con.del(stream_id).unwrap();  
        {
            let mut sender = Sender::connect("redis://127.0.0.1/", stream_id).unwrap();
            sender.send("","abc123").unwrap();
            let mut receiver = Receiver::connect("redis://127.0.0.1/", stream_id, "-").unwrap();
            let (k, v) = receiver.recv().unwrap();
            assert_eq!(k, b"");
            assert_eq!(v, b"abc123");
        }
        let _: () = con.del(stream_id).unwrap();  
    }
}