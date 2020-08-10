use btoi::btou;
use nom::{
    branch::alt,
    bytes::streaming::{tag, take, take_until, take_while1, take_while_m_n},
    character::{is_digit, streaming::crlf},
    combinator::{map, map_res, opt, value},
    multi::fold_many0,
    sequence::{preceded, terminated, tuple},
    IResult,
};

use super::{ErrorKind, Response, Status, Value};

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
            value(ErrorKind::Generic, tag(b"ERROR")),
            map_res(preceded(tag(b"CLIENT_ERROR "), take_until("\r\n")), |s| {
                std::str::from_utf8(s).map(|s| ErrorKind::Client(s))
            }),
            map_res(preceded(tag(b"SERVER_ERROR "), take_until("\r\n")), |s| {
                std::str::from_utf8(s).map(|s| ErrorKind::Server(s))
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

fn parse_ascii_incrdecr(buf: &[u8]) -> IResult<&[u8], Response> {
    terminated(map(parse_ascii_u64, Response::IncrDecr), crlf)(buf)
}

fn is_key_char(chr: u8) -> bool {
    chr > 32 && chr < 127
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
            key,
            cas,
            flags,
            data,
        },
    ))
}

fn parse_ascii_data(buf: &[u8]) -> IResult<&[u8], Response> {
    let values = map(
        fold_many0(parse_ascii_value, None, |xs, x| {
            let mut xs = xs.unwrap_or_else(|| Vec::new());
            xs.push(x);
            Some(xs)
        }),
        Response::Data,
    );

    terminated(values, tag("END\r\n"))(buf)
}

pub fn parse_ascii_response(buf: &[u8]) -> IResult<&[u8], Response> {
    alt((
        parse_ascii_status,
        parse_ascii_error,
        parse_ascii_incrdecr,
        parse_ascii_data,
    ))(buf)
}

#[cfg(test)]
mod tests {
    use super::{parse_ascii_response, ErrorKind, Response, Status, Value};
    use lazy_static::lazy_static;

    static FOO_KEY: &[u8] = b"foo";
    static BAR_KEY: &[u8] = b"bar";
    static FOO_STR: &str = "foo";
    static BAR_STR: &str = "bar";
    static HELLO_WORLD_DATA: &[u8] = b"hello world";

    lazy_static! {
        // (buffer to parse, expected number of bytes read, expected response)
        static ref VALID_CASES: Vec<(&'static [u8], usize, Response<'static>)> = {
            vec![
                // Normal examples: no dangling data, no curveballs.
                (b"STORED\r\n", 8, Response::Status(Status::Stored)),
                (b"NOT_STORED\r\n", 12, Response::Status(Status::NotStored)),
                (b"DELETED\r\n", 9, Response::Status(Status::Deleted)),
                (b"TOUCHED\r\n", 9, Response::Status(Status::Touched)),
                (b"EXISTS\r\n", 8, Response::Status(Status::Exists)),
                (b"NOT_FOUND\r\n", 11, Response::Status(Status::NotFound)),
                (b"ERROR\r\n", 7, Response::Status(Status::Error(ErrorKind::Generic))),
                (b"CLIENT_ERROR foo\r\n", 18, Response::Status(Status::Error(ErrorKind::Client(FOO_STR)))),
                (b"SERVER_ERROR bar\r\n", 18, Response::Status(Status::Error(ErrorKind::Server(BAR_STR)))),
                (b"42\r\n", 4, Response::IncrDecr(42)),
                (b"END\r\n", 5, Response::Data(None)),
                (b"VALUE foo 42 11\r\nhello world\r\nEND\r\n", 35, Response::Data(Some(
                    vec![Value { key: FOO_KEY, flags: 42, cas: None, data: HELLO_WORLD_DATA }]
                ))),
                (b"VALUE foo 42 11\r\nhello world\r\nVALUE bar 43 11 15\r\nhello world\r\nEND\r\n", 68,
                    Response::Data(Some(
                        vec![
                            Value { key: FOO_KEY, flags: 42, cas: None, data: HELLO_WORLD_DATA },
                            Value { key: BAR_KEY, flags: 43, cas: Some(15), data: HELLO_WORLD_DATA },
                        ]
                    ))
                ),
            ]
        };
    }

    #[test]
    fn test_complete_parsing() {
        // We assume all data has arrived for these tests.
        for (data, data_read, expected) in VALID_CASES.iter() {
            let (remaining, result) = parse_ascii_response(data).unwrap();

            assert_eq!(&result, expected);
            assert_eq!(data.len() - remaining.len(), *data_read);
        }
    }

    #[test]
    fn test_incomplete_parsing() {
        // For each case, we slice down the input data and assert that until we feed the entire
        // buffer, we don't get a valid response.
        for (data, data_read, expected) in VALID_CASES.iter() {
            let mut i = 0;
            while i < *data_read {
                let subbuf = &data[..i];
                assert!(parse_ascii_response(subbuf).is_err());
                i += 1;
            }

            let (remaining, result) = parse_ascii_response(data).unwrap();
            assert_eq!(&result, expected);
            assert_eq!(data.len() - remaining.len(), *data_read);
        }
    }
}
