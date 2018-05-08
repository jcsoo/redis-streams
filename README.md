# Redis Stream Support for Rust

Rust support for [Redis Streams](https://github.com/redis/redis-rcp/blob/master/RCP11.md)

See Antirez's blog posts about Redis Streams:

- [Streams: a new general purpose data structure in Redis.](http://antirez.com/news/114)
- [An update on Redis Streams development](http://antirez.com/news/116)

To use Redis Streams, you must have a version of Redis installed with stream support enabled. It is planned
for streams to appear in Redis 5.0.

Example:

```
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
```