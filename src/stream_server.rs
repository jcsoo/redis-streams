use super::redis::*;
use super::RedisStream;
use super::{Stream, Entry};

use std::vec;
use std::collections::HashMap;

pub struct StreamServer {
    conn: Connection,
    streams: HashMap<String, String>,
}

impl StreamServer {
    pub fn connect(url: &str) -> Result<Self, RedisError> {
        let client = Client::open(url)?;
        let conn = client.get_connection()?;
        Ok(Self::new(conn))
    }

    pub fn new(conn: Connection) -> Self {
        let streams = HashMap::new();
        StreamServer { conn, streams }
    }

    pub fn xadd(&mut self, stream_id: &str, key: &[u8], value: &[u8]) -> Result<String, RedisError> {
        self.conn.xadd(stream_id, key, value)
    }

    pub fn xadd_maxlen(&mut self, stream_id: &str, max_len: usize, key: &[u8], value: &[u8]) -> Result<String, RedisError> {
        self.conn.xadd_maxlen(stream_id, max_len, key, value)
    }


    pub fn register(&mut self, stream_id: &str, entry_id: &str) -> Option<String> {
        self.streams.insert(String::from(stream_id), String::from(entry_id))
    }

    pub fn xread(&mut self, block_ms: u32) -> Result<StreamIterator, RedisError> {        
        let mut cmd = cmd("XREAD");
        cmd.arg("BLOCK").arg(block_ms);
        cmd.arg("STREAMS");
        for (k, v) in self.streams.iter() {            
            cmd.arg(k).arg(v);
        }
        let v: Option<Vec<Stream>> = cmd.query(&self.conn)?;
        if let Some(streams) = v {
            for stream in streams.iter() {  
                let stream_id: String = stream.id().unwrap();
                let mut entry_id: String = String::new();
                for entry in stream.entries() {
                    entry_id = entry.id().unwrap();                    
                }
                self.streams.insert(stream_id, entry_id);
            }
            Ok(StreamIterator { inner: streams.into_iter() })
        } else {
            let mut empty = Vec::new();
            Ok(StreamIterator { inner: empty.into_iter() })
        }
    }

    pub fn now(&self) -> String {
        use std::time::{SystemTime, UNIX_EPOCH};
        let start = SystemTime::now();
        let since_the_epoch = start.duration_since(UNIX_EPOCH)
            .expect("Time went backwards");        
        let in_ms = since_the_epoch.as_secs() * 1000 +
            since_the_epoch.subsec_nanos() as u64 / 1_000_000;            
        format!("{}-0", in_ms - 1)
    }
}

pub struct StreamIterator {
    inner: vec::IntoIter<Stream>,
}

impl Iterator for StreamIterator {
    type Item = (String, EntryIterator);
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(stream) = self.inner.next() {
            let stream_id = stream.id().unwrap();
            let entries = stream.entries;
            Some((stream_id, EntryIterator { inner: entries.into_iter() }))
        } else {
            None
        }
    }
}

pub struct EntryIterator {
    inner: vec::IntoIter<Entry>,
}

impl Iterator for EntryIterator {
    type Item = (String, KeyValueIterator);
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(entry) = self.inner.next() {
            let entry_id = entry.id().unwrap();
            let key_values = entry.key_values;
            Some((entry_id, KeyValueIterator { inner: key_values.into_iter() }))
        } else {
            None
        }
    }
}

pub struct KeyValueIterator {
    inner: vec::IntoIter<Value>,
}

impl Iterator for KeyValueIterator {
    type Item = (Vec<u8>, Vec<u8>);
    fn next(&mut self) -> Option<Self::Item> {
        let k: Vec<u8> = if let Some(Value::Data(k)) = self.inner.next() {
            k
        } else {
            return None
        };
        let v: Vec<u8> = if let Some(Value::Data(v)) = self.inner.next() {
            v
        } else {
            return None
        };
        Some((k, v))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_server() {
        let client = Client::open("redis://127.0.0.1/").unwrap();
        let con = client.get_connection().unwrap();        
        let _: () = con.del("_test_abc").unwrap();  
        {
            let mut s = StreamServer::connect("redis://127.0.0.1/").unwrap();
            let now = s.now();
            println!("now: {}", now);
            s.register("_test_abc", &now);
            let v = s.xadd("_test_abc", b"key", b"value").unwrap();
            println!("v: {}", v);

            for (stream_id, entries) in s.xread(0).unwrap() {
                println!("{}", stream_id);
                for (entry_id, key_values) in entries {
                    println!("  {}", entry_id);
                    for (k, v) in key_values {
                        println!("     {}: {}", String::from_utf8_lossy(&k), String::from_utf8_lossy(&v));
                    }

                }
            }

            println!("---");

            let v = s.xadd("_test_abc", b"key", b"value2").unwrap();
            println!("v: {}", v);

            for (stream_id, entries) in s.xread(0).unwrap() {
                println!("{}", stream_id);
                for (entry_id, key_values) in entries {
                    println!("  {}", entry_id);
                    for (k, v) in key_values {
                        println!("     {}: {}", String::from_utf8_lossy(&k), String::from_utf8_lossy(&v));
                    }

                }
            }

        }
        let con = client.get_connection().unwrap();        
        let _: () = con.del("_test_abc").unwrap();  

    }
}