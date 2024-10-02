use std::convert::TryInto;

use nom::{
    branch::alt,
    bytes::streaming::{tag, take, take_while1},
    character::{
        complete::digit1,
        streaming::{crlf, space0, space1},
    },
    combinator::{map, map_res, value},
    multi::many0,
    sequence::{pair, preceded, terminated, tuple},
    IResult,
};

use super::{parse_u32, ErrorKind, MetaValue, Response, Status, MemcachedValue};

pub fn parse_meta_status(buf: &[u8]) -> IResult<&[u8], Response> {
    terminated(
        alt((
            value(Response::Status(Status::NotStored), tag(b"NS")),
            value(Response::Status(Status::Deleted), tag(b"DE")),
            value(Response::Status(Status::Touched), tag(b"TO")),
            value(Response::Status(Status::Exists), tag(b"EX")),
            value(Response::Status(Status::NotFound), tag(b"NF")),
        )),
        crlf,
    )(buf)
}

pub fn parse_meta_set_status(buf: &[u8]) -> IResult<&[u8], Response> {
    alt((value(Response::Status(Status::Stored), tag(b"HD")),))(buf)
}

pub fn parse_meta_get_status(buf: &[u8]) -> IResult<&[u8], Response> {
    alt((
        value(Response::Status(Status::Exists), tag(b"VA ")),
        value(Response::Status(Status::Exists), tag(b"HD ")),
        value(Response::Status(Status::NotFound), tag(b"EN\r\n")),
    ))(buf)
}

pub fn parse_meta_response(buf: &[u8]) -> Result<Option<(usize, Response)>, ErrorKind> {
    let bufn = buf.len();
    let result = alt((
        parse_meta_status,
        |input| parse_meta_get_data_value(input),
        |input| parse_meta_set_data_value(input),
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

fn parse_meta_set_data_value(buf: &[u8]) -> IResult<&[u8], Response> {
    let (input, success) = parse_meta_set_status(buf)?;
    match success {
        Response::Status(Status::Stored) => {
            // TODO: dont' call if there are no flags
            let (input, meta_values_array) = parse_meta_tag_values_as_slice(input)?;

            let mut value = MetaValue::default();

            for (flag, meta_value) in meta_values_array {
                match flag {
                    b'O' => value.opaque_token = Some(meta_value.to_vec()),
                    b'c' => {
                        value.cas =
                            Some(u64::from_be_bytes(meta_value.try_into().unwrap_or([0; 8])));
                    }
                    _ => {}
                }
            }

            let (input, _) = tag("\r\n")(input)?;
            Ok((input, Response::Data(Some(vec![MemcachedValue::MetaValue(value)]))))
        }
        _ => Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Eof,
        ))),
    }
}

// example meta_get command:
// mg meta-get-test-key v h l t
// so it has flags of v h l t
// meta_get example response:
// VA 10 h1 l56 t2179
// test-value
// this method should return a response with a value object like like:
// Value {
//     key: "meta-get-test-key",
//     cas: None,
//     flags: 0,
//     data: "test-value",
//     meta_values: Some(MetaValue {
//         h: true,
//         l: 56,
//         t: 2179,
//     }),
// }
fn parse_meta_get_data_value(buf: &[u8]) -> IResult<&[u8], Response> {
    let (input, success) = parse_meta_get_status(buf)?;
    match success {
        Response::Status(Status::Exists) => {
            let (input, size) = parse_u32(input)?;
            let (input, _tag) = tag(" ")(input)?;
            let (input, meta_values_array) = parse_meta_tag_values_as_u32(input)?;
            // remove the \r\n from the input
            let (input, _) = crlf(input)?;
            let (input, data) = take_until_size(input, size)?;

            let mut meta_values = MetaValue::default();
            for (flag, value) in meta_values_array {
                match flag {
                    b'h' => meta_values.hit_before = Some(value != 0),
                    b'l' => meta_values.last_accessed = Some(value as u64),
                    b't' => meta_values.ttl_remaining = Some(value as i64),
                    _ => {}
                }
            }

            // TODO: hmmm we don't have the key in the response anymore, should we return none and the caller maps it back?
            // or we need to pass the key into the parser...
            // slowly this is pointing towards a different value type all together for meta
            let value = MemcachedValue::MetaValue(MetaValue {
                key: Some(b"key".to_vec()),
                cas: None,
                meta_flags: Some(0),
                data: Some(data),
                hit_before: meta_values.hit_before,
                last_accessed: meta_values.last_accessed,
                ttl_remaining: meta_values.ttl_remaining,
                opaque_token: meta_values.opaque_token,
            });

            Ok((input, Response::Data(Some(vec![value]))))
        }
        Response::Status(Status::NotFound) => Ok((input, Response::Status(Status::NotFound))),
        _ => {
            // unexpected response code, should never happen, bail
            Err(nom::Err::Error(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Eof,
            )))
        }
    }
}

pub fn take_until_size(mut buf: &[u8], byte_size: u32) -> IResult<&[u8], Vec<u8>> {
    let mut data = Vec::with_capacity(byte_size as usize);
    while (data.len() as u32) < byte_size {
        let (remaining, chunk) = take_while1(|c| c != b'\r')(buf)?;
        data.extend_from_slice(chunk);
        buf = remaining;
        if data.len() as u32 == byte_size {
            // break out of the loop
            break;
        } else {
            let (remaining, chunk) = tag("\r\n")(buf)?;
            data.extend_from_slice(chunk);
            buf = remaining;
        }
    }
    // If we have reached byte_size, consume '\r\n' and return the data
    let (buf, _) = tag("\r\n")(buf)?;
    Ok((buf, data))
}

fn parse_meta_tag_values_as_u32(input: &[u8]) -> IResult<&[u8], Vec<(u8, u32)>> {
    many0(pair(
        preceded(space0, map(take(1usize), |s: &[u8]| s[0])),
        map_res(digit1, |s: &[u8]| {
            std::str::from_utf8(s).unwrap().parse::<u32>()
        }),
    ))(input)
}

fn parse_meta_tag_values_as_slice(input: &[u8]) -> IResult<&[u8], Vec<(u8, &[u8])>> {
    if input.is_empty() || input == b"\r\n" {
        Ok((input, Vec::new()))
    } else {
        map(
            tuple((
                space1,
                map(take(1usize), |s: &[u8]| s[0]),
                take_while1(|c: u8| c != b'\r'),
            )),
            |(_, flag, value)| vec![(flag, value)],
        )(input)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::parse_u32;
    use std::str;

    #[test]
    fn test_parse_meta_tag_values_as_slice() {
        let input = b" Oopaque-token\r\n";
        let (remaining, result) = parse_meta_tag_values_as_slice(input).unwrap();

        let token: &[u8] = "opaque-token".as_bytes();
        assert_eq!(result, vec![(b'O', token)]);
        assert_eq!(remaining, b"\r\n");

        // Test with empty input
        let empty_input = b"";
        let (remaining, result) = parse_meta_tag_values_as_slice(empty_input).unwrap();
        assert_eq!(result, vec![]);
        assert_eq!(remaining, b"");

        // Test with new line
        let empty_input = b"\r\n";
        let (remaining, result) = parse_meta_tag_values_as_slice(empty_input).unwrap();
        assert_eq!(result, vec![]);
        assert_eq!(remaining, b"\r\n");
    }

    #[test]
    fn test_parse_u32() {
        use nom::Err;

        let test_cases = [("0", 0), ("1", 1), ("42", 42), ("4294967295", u32::MAX)];

        for (input, expected) in test_cases.iter() {
            match parse_u32(input.as_bytes()) {
                Ok((remaining, result)) => {
                    assert_eq!(result, *expected);
                    assert!(remaining.is_empty());
                }
                Err(Err::Incomplete(_)) => {
                    // This is expected for a streaming parser
                    // We can try again with an extra character to signal end of input
                    let mut extended_input = input.to_string();
                    extended_input.push(' ');
                    match parse_u32(extended_input.as_bytes()) {
                        Ok((remaining, result)) => {
                            assert_eq!(result, *expected);
                            assert_eq!(remaining, b" ");
                        }
                        Err(e) => panic!("Failed on extended input {}: {:?}", extended_input, e),
                    }
                }
                Err(e) => panic!("Unexpected error for input {}: {:?}", input, e),
            }
        }

        // Test error cases
        assert!(matches!(parse_u32(b""), Err(Err::Incomplete(_))));
        assert!(parse_u32(b"4294967296 ").is_err()); // Overflow
        assert!(parse_u32(b"abc ").is_err());
    }

    #[test]
    fn test_parse_meta_tag_values() {
        let input = b"h1 l56 t2179\r\ntest-value";
        let (remaining, result) = parse_meta_tag_values_as_u32(input).unwrap();

        assert_eq!(result, vec![(b'h', 1), (b'l', 56), (b't', 2179),]);
        assert_eq!(remaining, b"\r\ntest-value");
    }

    #[test]
    fn test_parse_meta_tag_values_order_does_not_matter() {
        let input = b"t2179 h1 l56\r\ntest-value";
        let (remaining, result) = parse_meta_tag_values_as_u32(input).unwrap();

        assert_eq!(result, vec![(b't', 2179), (b'h', 1), (b'l', 56),]);
        assert_eq!(remaining, b"\r\ntest-value");
    }

    #[test]
    fn test_parse_meta_tag_values_handles_unknown_tags() {
        let input = b"h1 l56 t2179 x100\r\ntest-value";
        let (remaining, result) = parse_meta_tag_values_as_u32(input).unwrap();

        assert_eq!(
            result,
            vec![(b'h', 1), (b'l', 56), (b't', 2179), (b'x', 100),]
        );
        assert_eq!(remaining, b"\r\ntest-value");
    }

    #[test]
    fn test_parse_meta_get_data_value() {
        let input = b"VA 10 h1 l56 t2179\r\ntest-value\r\n";
        let (remaining, response) = parse_meta_get_data_value(input).unwrap();

        assert_eq!(remaining, b"");

        match response {
            Response::Data(Some(values)) => {
                assert_eq!(values.len(), 1);
                let value = &values[0];

                match value {
                    MemcachedValue::MetaValue(meta_value) => {
                        assert_eq!(
                            str::from_utf8(meta_value.data.as_ref().unwrap()).unwrap(),
                            "test-value"
                        );
                        assert_eq!(meta_value.cas, None);
                        assert_eq!(meta_value.hit_before, Some(true));
                        assert_eq!(meta_value.last_accessed, Some(56));
                        assert_eq!(meta_value.ttl_remaining, Some(2179));
                    }
                    _ => panic!("Expected MemcachedValue::MetaValue, got something else"),
                }
            }
            _ => panic!("Expected Response::Data, got something else"),
        }
    }

    #[test]
    fn test_parse_meta_get_data_value_tag_order_does_not_matter() {
        let input = b"VA 10 t2179 h1 l56\r\ntest-value\r\n";
        let (remaining, response) = parse_meta_get_data_value(input).unwrap();

        assert_eq!(remaining, b"");

        match response {
            Response::Data(Some(values)) => {
                assert_eq!(values.len(), 1);
                let value = &values[0];

                match value {
                    MemcachedValue::MetaValue(meta_value) => {
                        assert_eq!(
                            str::from_utf8(meta_value.data.as_ref().unwrap()).unwrap(),
                            "test-value"
                        );
                        assert_eq!(meta_value.cas, None);
                        assert_eq!(meta_value.hit_before, Some(true));
                        assert_eq!(meta_value.last_accessed, Some(56));
                        assert_eq!(meta_value.ttl_remaining, Some(2179));
                    }
                    _ => panic!("Expected MemcachedValue::MetaValue, got something else"),
                }
            }
            _ => panic!("Expected Response::Data, got something else"),
        }
    }

    #[test]
    fn test_parse_meta_get_data_value_ignores_unknown_tags() {
        let input = b"VA 10 h1 l56 t2179 x100\r\ntest-value\r\n";
        let (remaining, response) = parse_meta_get_data_value(input).unwrap();

        assert_eq!(remaining, b"");

        match response {
            Response::Data(Some(values)) => {
                assert_eq!(values.len(), 1);
                let value = &values[0];

                match value {
                    MemcachedValue::MetaValue(meta_value) => {
                        assert_eq!(
                            str::from_utf8(meta_value.data.as_ref().unwrap()).unwrap(),
                            "test-value"
                        );
                        assert_eq!(meta_value.cas, None);
                        assert_eq!(meta_value.hit_before, Some(true));
                        assert_eq!(meta_value.last_accessed, Some(56));
                        assert_eq!(meta_value.ttl_remaining, Some(2179));
                    }
                    _ => panic!("Expected MemcachedValue::MetaValue, got something else"),
                }
            }
            _ => panic!("Expected Response::Data, got something else"),
        }
    }

    #[test]
    fn test_parse_meta_get_data_value_cache_miss() {
        let input = b"EN\r\n";
        let (remaining, response) = parse_meta_get_data_value(input).unwrap();
        assert_eq!(remaining, b"");
        assert_eq!(response, Response::Status(Status::NotFound));
    }

    // Text lines are always terminated by \r\n. Unstructured data is _also_
    // terminated by \r\n, even though \r, \n or any other 8-bit characters
    // may also appear inside the data. Therefore, when a client retrieves
    // data from a server, it must use the length of the data block (which it
    // will be provided with) to determine where the data block ends, and not
    // the fact that \r\n follows the end of the data block, even though it
    // does.
    // https://github.com/memcached/memcached/blob/master/doc/protocol.txt
    #[test]
    fn test_parse_meta_get_data_value_handles_data_with_embedded_crlf() {
        let input = b"VA 12 h1 l56 t2179\r\ntest-\r\nvalue\r\n";
        let (remaining, response) = parse_meta_get_data_value(input).unwrap();

        assert_eq!(
            remaining,
            b"",
            "Remaining input should be empty but was {:?}",
            str::from_utf8(remaining).unwrap()
        );
        match response {
            Response::Data(Some(values)) => {
                assert_eq!(values.len(), 1);
                let value = &values[0];

                match value {
                    MemcachedValue::MetaValue(meta_value) => {
                        assert_eq!(
                            str::from_utf8(meta_value.data.as_ref().unwrap()).unwrap(),
                            "test-\r\nvalue"
                        );
                        assert_eq!(meta_value.cas, None);
                        assert_eq!(meta_value.hit_before, Some(true));
                        assert_eq!(meta_value.last_accessed, Some(56));
                        assert_eq!(meta_value.ttl_remaining, Some(2179));
                    }
                    _ => panic!("Expected MemcachedValue::MetaValue, got something else"),
                }
            }
            _ => panic!("Expected Response::Data, got something else"),
        }
    }
}
