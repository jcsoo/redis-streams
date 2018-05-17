//! Rust support for [Redis Streams](https://github.com/redis/redis-rcp/blob/master/RCP11.md)
//! 
//! [Streams: a new general purpose data structure in Redis.](http://antirez.com/news/114)
//! [An update on Redis Streams development](http://antirez.com/news/116)
//! 
//! To use Redis Streams, you must have a version of Redis installed with stream support enabled. It is planned
//! for streams to appear in Redis 5.0.

extern crate redis;
extern crate itertools;

use redis::*;
use std::slice;

pub fn xadd(stream: &str) -> Xadd {
    let mut cmd = cmd("XADD");
    cmd.arg(stream).arg("*");
    Xadd(cmd)
}

pub fn xadd_maxlen(stream: &str, max_len: usize) -> Xadd {
    let mut cmd = cmd("XADD");
    cmd.arg(stream).arg("MAXLEN").arg(max_len).arg("*");
    Xadd(cmd)
}

pub struct Xadd(Cmd);

impl Xadd {
    pub fn entry<K: ToRedisArgs, V: ToRedisArgs>(mut self, key: K, val: V) -> Self {
        self.0.arg(key).arg(val);
        self
    }

    pub fn query<RV: FromRedisValue>(&self, con: &Connection) -> RedisResult<RV> {
        self.0.query(con)
    }

    pub fn execute(&self, con: &Connection) {
        let _ : () = self.query(con).unwrap();
    }
}

pub trait RedisStream {
    fn xadd<S: ToRedisArgs, K: ToRedisArgs, V: ToRedisArgs, RV: FromRedisValue>(&mut self, stream: S, key: K, value: V) -> RedisResult<RV>;
    fn xadd_maxlen<S: ToRedisArgs, L: ToRedisArgs, K: ToRedisArgs, V: ToRedisArgs, RV: FromRedisValue>(&mut self, stream: S, max_len: L, key: K, value: V) -> RedisResult<RV>;
    fn xadd_multiple<S: ToRedisArgs, K: ToRedisArgs, V: ToRedisArgs, RV: FromRedisValue>(&mut self, stream: S, entries: &[(K, V)]) -> RedisResult<RV>;
    fn xadd_maxlen_multiple<S: ToRedisArgs, L: ToRedisArgs, K: ToRedisArgs, V: ToRedisArgs, RV: FromRedisValue>(&mut self, stream: S, max_len: L, entries: &[(K, V)]) -> RedisResult<RV>;

    fn xlen<S: ToRedisArgs, RV: FromRedisValue>(&mut self, stream: S) -> RedisResult<RV>;

    fn xrange<S: ToRedisArgs, A: ToRedisArgs, B: ToRedisArgs, RV: FromRedisValue>(&mut self, stream: S, start: A, stop: B) -> RedisResult<RV>;
    fn xrange_count<S: ToRedisArgs, A: ToRedisArgs, B: ToRedisArgs, C: ToRedisArgs, RV: FromRedisValue>(&mut self, stream: S, start: A, stop: B, count: C) -> RedisResult<RV>;

    fn xrevrange<S: ToRedisArgs, A: ToRedisArgs, B: ToRedisArgs, RV: FromRedisValue>(&mut self, stream: S, start: A, stop: B) -> RedisResult<RV>;
    fn xrevrange_count<S: ToRedisArgs, A: ToRedisArgs, B: ToRedisArgs, C: ToRedisArgs, RV: FromRedisValue>(&mut self, stream: S, start: A, stop: B, count: C) -> RedisResult<RV>;

    fn xread<S: ToRedisArgs, I: ToRedisArgs, RV: FromRedisValue>(&mut self, stream: S, id: I) -> RedisResult<RV>;
    fn xread_multiple<RV: FromRedisValue>(&mut self, entries: &[(String, String)]) -> RedisResult<RV>;

    fn xread_block<B: ToRedisArgs, S: ToRedisArgs, I: ToRedisArgs, RV: FromRedisValue>(&mut self, block: B, stream: S, id: I) -> RedisResult<RV>;
    fn xread_block_multiple<B: ToRedisArgs, RV: FromRedisValue>(&mut self, block: B, entries: &[(String, String)]) -> RedisResult<RV>;

    fn xread_count<S: ToRedisArgs, I: ToRedisArgs, RV: FromRedisValue>(&mut self, count: isize, stream: S, id: I) -> RedisResult<RV>;
    fn xread_count_multiple<RV: FromRedisValue>(&mut self, count: isize, entries: &[(String,  String)]) -> RedisResult<RV>;

    fn xread_count_block<B: ToRedisArgs, S: ToRedisArgs, I: ToRedisArgs, RV: FromRedisValue>(&mut self, count: isize, block: B, stream: S, id: I) -> RedisResult<RV>;
    fn xread_count_block_multiple<B: ToRedisArgs, RV: FromRedisValue>(&mut self, count: isize, block: B, entries: &[(String, String)]) -> RedisResult<RV>;
}

impl RedisStream for redis::Connection {
    fn xadd<S: ToRedisArgs, K: ToRedisArgs, V: ToRedisArgs, RV: FromRedisValue>(&mut self, stream: S, key: K, value: V) -> RedisResult<RV> {
        cmd("XADD").arg(stream).arg("*").arg(key).arg(value).query(self)
    }

    fn xadd_maxlen<S: ToRedisArgs, L: ToRedisArgs, K: ToRedisArgs, V: ToRedisArgs, RV: FromRedisValue>(&mut self, stream: S, max_len: L, key: K, value: V) -> RedisResult<RV> {
        cmd("XADD").arg(stream).arg("MAXLEN").arg(max_len).arg("*").arg(key).arg(value).query(self)
    }

    fn xadd_multiple<S: ToRedisArgs, K: ToRedisArgs, V: ToRedisArgs, RV: FromRedisValue>(&mut self, stream: S, entries: &[(K, V)]) -> RedisResult<RV> {
        cmd("XADD").arg(stream).arg("*").arg(entries).query(self)
    }

    fn xadd_maxlen_multiple<S: ToRedisArgs, L: ToRedisArgs, K: ToRedisArgs, V: ToRedisArgs, RV: FromRedisValue>(&mut self, stream: S, max_len: L, entries: &[(K, V)]) -> RedisResult<RV> {
        cmd("XADD").arg(stream).arg("MAXLEN").arg(max_len).arg("*").arg(entries).query(self)
    }

    fn xlen<S: ToRedisArgs, RV: FromRedisValue>(&mut self, stream: S) -> RedisResult<RV> {
        cmd("XLEN").arg(stream).query(self)
    }

    fn xrange<S: ToRedisArgs, A: ToRedisArgs, B: ToRedisArgs, RV: FromRedisValue>(&mut self, stream: S, start: A, stop: B) -> RedisResult<RV> {
        cmd("XRANGE").arg(stream).arg(start).arg(stop).query(self)
    }

    fn xrange_count<S: ToRedisArgs, A: ToRedisArgs, B: ToRedisArgs, C: ToRedisArgs, RV: FromRedisValue>(&mut self, stream: S, start: A, stop: B, count: C) -> RedisResult<RV> {
        cmd("XRANGE").arg(stream).arg(start).arg(stop).arg("COUNT").arg(count).query(self)
    }

    fn xrevrange<S: ToRedisArgs, A: ToRedisArgs, B: ToRedisArgs, RV: FromRedisValue>(&mut self, stream: S, start: A, stop: B) -> RedisResult<RV> {
        cmd("XREVRANGE").arg(stream).arg(start).arg(stop).query(self)
    }

    fn xrevrange_count<S: ToRedisArgs, A: ToRedisArgs, B: ToRedisArgs, C: ToRedisArgs, RV: FromRedisValue>(&mut self, stream: S, start: A, stop: B, count: C) -> RedisResult<RV> {
        cmd("XREVRANGE").arg(stream).arg(start).arg(stop).arg("COUNT").arg(count).query(self)
    }    

    fn xread<S: ToRedisArgs, I: ToRedisArgs, RV: FromRedisValue>(&mut self, stream: S, id: I) -> RedisResult<RV> {
        cmd("XREAD").arg("STREAMS").arg(stream).arg(id).query(self)
    }

    fn xread_multiple<RV: FromRedisValue>(&mut self, entries: &[(String, String)]) -> RedisResult<RV> {
        let mut cmd = cmd("XREAD");
        cmd.arg("STREAMS");
        for &(ref s, _) in entries.iter() {
            cmd.arg(s);
        }
        for &(_, ref i) in entries.iter() {
            cmd.arg(i);
        }
        cmd.query(self)       
    }

    fn xread_block<B: ToRedisArgs, S: ToRedisArgs, I: ToRedisArgs, RV: FromRedisValue>(&mut self, block: B, stream: S, id: I) -> RedisResult<RV> {
        cmd("XREAD").arg("BLOCK").arg(block).arg("STREAMS").arg(stream).arg(id).query(self)
    }

    fn xread_block_multiple<B: ToRedisArgs, RV: FromRedisValue>(&mut self, block: B, entries: &[(String, String)]) -> RedisResult<RV> {
        let mut cmd = cmd("XREAD");
        cmd.arg("BLOCK").arg(block).arg("STREAMS");
        for &(ref s, _) in entries.iter() {
            cmd.arg(s);
        }
        for &(_, ref i) in entries.iter() {
            cmd.arg(i);
        }
        cmd.query(self)   
    }

    fn xread_count<S: ToRedisArgs, I: ToRedisArgs, RV: FromRedisValue>(&mut self, count: isize, stream: S, id: I) -> RedisResult<RV> {
        cmd("XREAD").arg("COUNT").arg(count).arg("STREAMS").arg(stream).arg(id).query(self)
    }

    fn xread_count_multiple<RV: FromRedisValue>(&mut self, count: isize, entries: &[(String, String)]) -> RedisResult<RV> {
        let mut cmd = cmd("XREAD");
        cmd.arg("COUNT").arg(count).arg("STREAMS");
        for &(ref s, _) in entries.iter() {
            cmd.arg(s);
        }
        for &(_, ref i) in entries.iter() {
            cmd.arg(i);
        }
        cmd.query(self)         }


    fn xread_count_block<B: ToRedisArgs, S: ToRedisArgs, I: ToRedisArgs, RV: FromRedisValue>(&mut self, count: isize, block: B, stream: S, id: I) -> RedisResult<RV> {
        cmd("XREAD").arg("COUNT").arg(count).arg("BLOCK").arg(block).arg("STREAMS").arg(stream).arg(id).query(self)
    }

    fn xread_count_block_multiple<B: ToRedisArgs, RV: FromRedisValue>(&mut self, count: isize, block: B, entries: &[(String, String)]) -> RedisResult<RV> {
        let mut cmd = cmd("XREAD");
        cmd.arg("COUNT").arg(count).arg("BLOCK").arg(block).arg("STREAMS");
        for &(ref s, _) in entries.iter() {
            cmd.arg(s);
        }
        for &(_, ref i) in entries.iter() {
            cmd.arg(i);
        }
        cmd.query(self)        
    }    
}

pub struct Stream {
    id: Value,
    entries: Vec<Entry>,
}

impl Stream {
    pub fn id<RV: FromRedisValue>(&self) -> RedisResult<RV> {
        from_redis_value(&self.id)
    }

    pub fn entries(&self) -> slice::Iter<Entry> {
        self.entries.iter()
    }
}

impl FromRedisValue for Stream {
    fn from_redis_value(v: &Value) -> RedisResult<Stream> {
        let (id, entries) : (Value, Vec<Entry>) = from_redis_value(v)?;
        Ok(Stream {
            id: id,
            entries: entries,
        })
    }
}

pub struct Entry {
    id: Value,
    key_values: Vec<Value>,
}

impl FromRedisValue for Entry {
    fn from_redis_value(v: &Value) -> RedisResult<Entry> {
        let (id, key_values) : (Value, Vec<Value>) = from_redis_value(v)?;
        Ok(Entry {
            id: id,
            key_values: key_values,
        })
    }    
}

impl Entry {
    pub fn id<RV: FromRedisValue>(&self) -> RedisResult<RV> {
        from_redis_value(&self.id)
    }

    pub fn key_values(&self) -> slice::Iter<Value> {
        self.key_values.iter()
    }
}


pub trait HandleEntry {
    type Error;
    fn handle_entry(&mut self, stream: &str, entry: &Entry) -> Result<(), Self::Error>;
    fn handle_timeout(&mut self, _duration: u32) -> Result<(), Self::Error> { Ok(()) }
}

// pub struct KeyValue {
//     key: Value,
//     value: Value,
// }

// impl KeyValue {
//     pub fn get<K: FromRedisValue, V: FromRedisValue>(&self) -> RedisResult<(K,V)> {
//         let k = from_redis_value(&self.key)?;
//         let v = from_redis_value(&self.value)?;
//         Ok((k, v))
//     }

//     pub fn key<RV: FromRedisValue>(&self) -> RedisResult<RV> {
//         from_redis_value(&self.key)
//     }

//     pub fn value<RV: FromRedisValue>(&self) -> RedisResult<RV> {
//         from_redis_value(&self.value)
//     }
// }

// impl FromRedisValue for KeyValue {
//     fn from_redis_value(v: &Value) -> RedisResult<KeyValue> {
//         let (key, value) : (Value, Value) = from_redis_value(v)?;
//         Ok(KeyValue {
//             key: key,
//             value: value
//         })
//     }    
// }


#[cfg(test)]
mod tests {
    use super::*;
    use itertools::Itertools;

    #[test]
    fn test_abc() {
        let client = redis::Client::open("redis://127.0.0.1/").unwrap();
        let con = client.get_connection().unwrap();
        let _: () = con.set("abc", "def").unwrap();
        let v: String = con.get("abc").unwrap();
        assert!(v == "def");
        let _: () = con.del("abc").unwrap();
    }

    #[test]
    fn test_stream() {
        let client = redis::Client::open("redis://127.0.0.1/").unwrap();
        let mut con = client.get_connection().unwrap();
        let _: () = con.del("mystream").unwrap();
        
        // XADD mystream * abc 123 def 456

        let s: String = con.xadd_multiple("mystream", &[("abc","123"), ("def", "456")]).unwrap();

        // XLEN mystream

        let v: u32 = con.xlen("mystream").unwrap();

        assert_eq!(v, 1);

        // XREAD COUNT 1 STREAMS mystream -

        let v: Option<Vec<Stream>> = con.xread_count(1, "mystream", "-").unwrap();

        // bulk(
        //    bulk(
        //       string-data('"mystream"'), 
        //       bulk(
        //         bulk(
        //           status("1515790156195-0"), 
        //           bulk(
        //             string-data('"abc"'), 
        //             string-data('"123"')
        //             string-data('"def"'), 
        //             string-data('"456"')
        //  )))))

        if let Some(ref streams) = v {
            for stream in streams.iter() {
                let stream_id: String = stream.id().unwrap();
                assert_eq!(stream_id, "mystream");

                for entry in stream.entries() {
                    let entry_id: String = entry.id().unwrap();
                    assert_eq!(entry_id, s);

                    for (n, (k, v)) in entry.key_values().tuples().enumerate() {
                        let k: String = from_redis_value(k).unwrap();
                        let v: String = from_redis_value(v).unwrap();

                        match n {
                            0 => {
                                assert_eq!(k, "abc");
                                assert_eq!(v, "123");
                            },
                            1 => {
                                assert_eq!(k, "def");
                                assert_eq!(v, "456");
                            },
                            _ => panic!("unexpected key value entry"),
                        }
                    }
                }
            }
        }

        let _: () = con.del("mystream").unwrap();
    }
}