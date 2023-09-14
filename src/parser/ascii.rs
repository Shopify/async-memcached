use btoi::{btoi, btou};
use nom::{
    branch::alt,
    bytes::streaming::{tag, take, take_until, take_while1, take_while_m_n},
    character::{
        is_digit,
        streaming::{crlf, newline},
    },
    combinator::{map, map_res, opt, value},
    multi::fold_many0,
    sequence::{preceded, terminated, tuple},
    IResult,
};
use std::str::Utf8Error;

use super::{ErrorKind, KeyMetadata, MetadumpResponse, Response, StatsResponse, Status, Value};

pub fn parse_ascii_status(buf: &[u8]) -> IResult<&[u8], Response> {
    terminated(
        alt((
            value(Response::Status(Status::Stored), tag(b"STORED")),
            value(Response::Status(Status::NotStored), tag(b"NOT_STORED")),
            value(Response::Status(Status::Deleted), tag(b"DELETED")),
            value(Response::Status(Status::Touched), tag(b"TOUCHED")),
            value(Response::Status(Status::Exists), tag(b"EXISTS")),
            value(Response::Status(Status::NotFound), tag(b"NOT_FOUND")),
        )),
        crlf,
    )(buf)
}

fn parse_ascii_error(buf: &[u8]) -> IResult<&[u8], Response> {
    let parser = terminated(
        alt((
            value(ErrorKind::NonexistentCommand, tag(b"ERROR")),
            map_res(preceded(tag(b"CLIENT_ERROR "), take_until("\r\n")), |s| {
                std::str::from_utf8(s).map(|s| ErrorKind::Client(s.to_string()))
            }),
            map_res(preceded(tag(b"SERVER_ERROR "), take_until("\r\n")), |s| {
                std::str::from_utf8(s).map(|s| ErrorKind::Server(s.to_string()))
            }),
        )),
        crlf,
    );

    map(parser, |e| Response::Status(Status::Error(e)))(buf)
}

fn parse_ascii_u32(buf: &[u8]) -> IResult<&[u8], u32> {
    map_res(take_while_m_n(1, 10, is_digit), btou)(buf)
}

fn parse_ascii_u64(buf: &[u8]) -> IResult<&[u8], u64> {
    map_res(take_while_m_n(1, 20, is_digit), btou)(buf)
}

fn parse_ascii_i64(buf: &[u8]) -> IResult<&[u8], i64> {
    map_res(take_while_m_n(1, 20, is_signed_digit), btoi)(buf)
}

fn parse_bool(buf: &[u8]) -> IResult<&[u8], bool> {
    alt((value(true, tag(b"yes")), value(false, tag(b"no"))))(buf)
}

fn parse_ascii_incrdecr(buf: &[u8]) -> IResult<&[u8], Response> {
    terminated(map(parse_ascii_u64, Response::IncrDecr), crlf)(buf)
}

fn is_key_char(chr: u8) -> bool {
    chr > 32 && chr < 127
}

fn is_signed_digit(chr: u8) -> bool {
    chr == 45 || (48..=57).contains(&chr)
}

fn parse_ascii_value(buf: &[u8]) -> IResult<&[u8], Value> {
    let kf = take_while1(is_key_char);
    let (buf, (_, key, _, flags, _, len, _, cas, _)) = tuple((
        // VALUE key flags data_len [cas id]\r\n
        // data block\r\n
        tag("VALUE "),
        kf,
        tag(" "),
        parse_ascii_u32,
        tag(" "),
        parse_ascii_u64,
        opt(tag(" ")),
        opt(parse_ascii_u64),
        crlf,
    ))(buf)?;
    let (buf, data) = terminated(take(len), crlf)(buf)?;
    Ok((
        buf,
        Value {
            key: key.to_vec(),
            cas,
            flags,
            data: data.to_vec(),
        },
    ))
}

fn parse_ascii_data(buf: &[u8]) -> IResult<&[u8], Response> {
    let values = map(
        fold_many0(parse_ascii_value, || { None }, |xs, x| {
            let mut xs: Vec<Value> = xs.unwrap_or_default();
            xs.push(x);
            Some(xs)
        }),
        Response::Data,
    );

    terminated(values, tag("END\r\n"))(buf)
}

pub fn parse_ascii_response(buf: &[u8]) -> Result<Option<(usize, Response)>, ErrorKind> {
    let bufn = buf.len();
    let result = alt((
        parse_ascii_status,
        parse_ascii_error,
        parse_ascii_incrdecr,
        parse_ascii_data,
    ))(buf);

    match result {
        Ok((left, response)) => {
            let n = bufn - left.len();
            Ok(Some((n, response)))
        }
        Err(nom::Err::Incomplete(_)) => Ok(None),
        Err(nom::Err::Error(e)) | Err(nom::Err::Failure(e)) => {
            Err(ErrorKind::Protocol(Some(e.code.description().to_string())))
        }
    }
}

fn parse_lru_crawler_errors(buf: &[u8]) -> IResult<&[u8], MetadumpResponse> {
    terminated(
        alt((
            map_res(preceded(tag(b"BUSY "), take_until("\r\n")), |s| {
                std::str::from_utf8(s).map(|s| MetadumpResponse::Busy(s.to_string()))
            }),
            map_res(preceded(tag(b"BADCLASS "), take_until("\r\n")), |s| {
                std::str::from_utf8(s).map(|s| MetadumpResponse::BadClass(s.to_string()))
            }),
        )),
        crlf,
    )(buf)
}

fn parse_lru_crawler_metadata(buf: &[u8]) -> IResult<&[u8], MetadumpResponse> {
    // key=boo exp=-1 la=1597801411 cas=157043 fetch=yes cls=1 size=75
    // key=foo exp=-1 la=1597801394 cas=63208 fetch=yes cls=1 size=73
    let (buf, (_, _, key, _, exp, _, la, _, cas, _, fetch, _, cls, _, size, _)) = tuple((
        opt(newline),
        tag("key="),
        take_while1(is_key_char),
        tag(" exp="),
        parse_ascii_i64,
        tag(" la="),
        parse_ascii_u64,
        tag(" cas="),
        parse_ascii_u64,
        tag(" fetch="),
        parse_bool,
        tag(" cls="),
        parse_ascii_u32,
        tag(" size="),
        parse_ascii_u32,
        newline,
    ))(buf)?;

    Ok((
        buf,
        MetadumpResponse::Entry(KeyMetadata {
            key: key.to_vec(),
            expiration: exp,
            last_accessed: la,
            cas,
            fetched: fetch,
            class_id: cls,
            size,
        }),
    ))
}

fn parse_stat_entry(buf: &[u8]) -> IResult<&[u8], StatsResponse> {
    terminated(
        map_res(
            tuple((
                tag("STAT "),
                take_while1(is_key_char),
                tag(" "),
                take_while1(is_key_char),
            )),
            |(_, key, _, value)| {
                let keystr = std::str::from_utf8(key)?;
                let valuestr = std::str::from_utf8(value)?;
                Ok::<_, Utf8Error>(StatsResponse::Entry(
                    keystr.to_string(),
                    valuestr.to_string(),
                ))
            },
        ),
        crlf,
    )(buf)
}

pub fn parse_ascii_metadump_response(
    buf: &[u8],
) -> Result<Option<(usize, MetadumpResponse)>, ErrorKind> {
    let bufn = buf.len();
    let result = alt((
        value(MetadumpResponse::End, tag(b"END\r\n")),
        parse_lru_crawler_errors,
        parse_lru_crawler_metadata,
    ))(buf);

    match result {
        Ok((left, response)) => {
            let n = bufn - left.len();
            Ok(Some((n, response)))
        }
        Err(nom::Err::Incomplete(_)) => Ok(None),
        Err(nom::Err::Error(e)) | Err(nom::Err::Failure(e)) => {
            Err(ErrorKind::Protocol(Some(e.code.description().to_string())))
        }
    }
}

pub fn parse_ascii_stats_response(buf: &[u8]) -> Result<Option<(usize, StatsResponse)>, ErrorKind> {
    let bufn = buf.len();
    let result = alt((value(StatsResponse::End, tag(b"END\r\n")), parse_stat_entry))(buf);

    match result {
        Ok((left, response)) => {
            let n = bufn - left.len();
            Ok(Some((n, response)))
        }
        Err(nom::Err::Incomplete(_)) => Ok(None),
        Err(nom::Err::Error(e)) | Err(nom::Err::Failure(e)) => {
            Err(ErrorKind::Protocol(Some(e.code.description().to_string())))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        parse_ascii_metadump_response, parse_ascii_response, parse_ascii_stats_response, ErrorKind,
        KeyMetadata, MetadumpResponse, Response, StatsResponse, Status, Value,
    };
    use lazy_static::lazy_static;

    static FOO_KEY: &[u8] = b"foo";
    static BAR_KEY: &[u8] = b"bar";
    static FOO_STR: &str = "foo";
    static BAR_STR: &str = "bar";
    static HELLO_WORLD_DATA: &[u8] = b"hello world";

    lazy_static! {
        // (buffer to parse, expected number of bytes read, expected response)
        static ref VALID_NORMAL_CASES: Vec<(&'static [u8], usize, Response)> = {
            vec![
                // Normal examples: no dangling data, no curveballs.
                (b"STORED\r\n", 8, Response::Status(Status::Stored)),
                (b"NOT_STORED\r\n", 12, Response::Status(Status::NotStored)),
                (b"DELETED\r\n", 9, Response::Status(Status::Deleted)),
                (b"TOUCHED\r\n", 9, Response::Status(Status::Touched)),
                (b"EXISTS\r\n", 8, Response::Status(Status::Exists)),
                (b"NOT_FOUND\r\n", 11, Response::Status(Status::NotFound)),
                (b"ERROR\r\n", 7, Response::Status(Status::Error(ErrorKind::NonexistentCommand))),
                (b"CLIENT_ERROR foo\r\n", 18, Response::Status(Status::Error(ErrorKind::Client(FOO_STR.to_string())))),
                (b"SERVER_ERROR bar\r\n", 18, Response::Status(Status::Error(ErrorKind::Server(BAR_STR.to_string())))),
                (b"42\r\n", 4, Response::IncrDecr(42)),
                (b"END\r\n", 5, Response::Data(None)),
                (b"VALUE foo 42 11\r\nhello world\r\nEND\r\n", 35, Response::Data(Some(
                    vec![Value { key: FOO_KEY.to_vec(), flags: 42, cas: None, data: HELLO_WORLD_DATA.to_vec() }]
                ))),
                (b"VALUE foo 42 11\r\nhello world\r\nVALUE bar 43 11 15\r\nhello world\r\nEND\r\n", 68,
                    Response::Data(Some(
                        vec![
                            Value { key: FOO_KEY.to_vec(), flags: 42, cas: None, data: HELLO_WORLD_DATA.to_vec() },
                            Value { key: BAR_KEY.to_vec(), flags: 43, cas: Some(15), data: HELLO_WORLD_DATA.to_vec() },
                        ]
                    ))
                ),
            ]
        };

        static ref VALID_METADUMP_CASES: Vec<(&'static [u8], usize, MetadumpResponse)> = {
            vec![
                // Normal examples: no dangling data, no curveballs.
                (b"END\r\n", 5, MetadumpResponse::End),
                (b"BUSY foobar\r\n", 13, MetadumpResponse::Busy("foobar".to_string())),
                (b"BADCLASS quux\r\n", 15, MetadumpResponse::BadClass("quux".to_string())),
                (b"key=foo exp=-1 la=1597801411 cas=157043 fetch=yes cls=1 size=75\n", 64, MetadumpResponse::Entry(KeyMetadata {
                    key: FOO_KEY.to_vec(),
                    expiration: -1,
                    last_accessed: 1597801411,
                    cas: 157043,
                    fetched: true,
                    class_id: 1,
                    size: 75,
                })),
                (b"\nkey=foo exp=-1 la=1597801411 cas=157043 fetch=yes cls=1 size=75\n", 65, MetadumpResponse::Entry(KeyMetadata {
                    key: FOO_KEY.to_vec(),
                    expiration: -1,
                    last_accessed: 1597801411,
                    cas: 157043,
                    fetched: true,
                    class_id: 1,
                    size: 75,
                })),
            ]
        };

        static ref VALID_STATS_CASES: Vec<(&'static [u8], usize, StatsResponse)> = {
            vec![
                // Normal examples: no dangling data, no curveballs.
                (b"END\r\n", 5, StatsResponse::End),
                (b"STAT foobar quux\r\n", 18, StatsResponse::Entry("foobar".to_string(), "quux".to_string())),
            ]
        };
    }

    #[test]
    fn test_regular_complete_parsing() {
        // We assume all data has arrived for these tests.
        for (data, data_read, expected) in VALID_NORMAL_CASES.iter() {
            let (n, result) = parse_ascii_response(data).unwrap().unwrap();

            assert_eq!(&result, expected);
            assert_eq!(n, *data_read);
        }
    }

    #[test]
    fn test_regular_incomplete_parsing() {
        // For each case, we slice down the input data and assert that until we feed the entire
        // buffer, we don't get a valid response.
        for (data, data_read, expected) in VALID_NORMAL_CASES.iter() {
            let mut i = 0;
            while i < *data_read {
                let subbuf = &data[..i];
                assert_eq!(parse_ascii_response(subbuf), Ok(None));
                i += 1;
            }

            let (n, result) = parse_ascii_response(data).unwrap().unwrap();
            assert_eq!(&result, expected);
            assert_eq!(n, *data_read);
        }
    }

    #[test]
    fn test_metadump_complete_parsing() {
        // We assume all data has arrived for these tests.
        for (data, data_read, expected) in VALID_METADUMP_CASES.iter() {
            let (n, result) = parse_ascii_metadump_response(data).unwrap().unwrap();

            assert_eq!(&result, expected);
            assert_eq!(n, *data_read);
        }
    }

    #[test]
    fn test_metadump_incomplete_parsing() {
        // For each case, we slice down the input data and assert that until we feed the entire
        // buffer, we don't get a valid response.
        for (data, data_read, expected) in VALID_METADUMP_CASES.iter() {
            let mut i = 0;
            while i < *data_read {
                let subbuf = &data[..i];
                assert_eq!(parse_ascii_metadump_response(subbuf), Ok(None));
                i += 1;
            }

            let (n, result) = parse_ascii_metadump_response(data).unwrap().unwrap();
            assert_eq!(&result, expected);
            assert_eq!(n, *data_read);
        }
    }

    #[test]
    fn test_stats_complete_parsing() {
        // We assume all data has arrived for these tests.
        for (data, data_read, expected) in VALID_STATS_CASES.iter() {
            let (n, result) = parse_ascii_stats_response(data).unwrap().unwrap();

            assert_eq!(&result, expected);
            assert_eq!(n, *data_read);
        }
    }

    #[test]
    fn test_stats_incomplete_parsing() {
        // For each case, we slice down the input data and assert that until we feed the entire
        // buffer, we don't get a valid response.
        for (data, data_read, expected) in VALID_STATS_CASES.iter() {
            let mut i = 0;
            while i < *data_read {
                let subbuf = &data[..i];
                assert_eq!(parse_ascii_stats_response(subbuf), Ok(None));
                i += 1;
            }

            let (n, result) = parse_ascii_stats_response(data).unwrap().unwrap();
            assert_eq!(&result, expected);
            assert_eq!(n, *data_read);
        }
    }
}
