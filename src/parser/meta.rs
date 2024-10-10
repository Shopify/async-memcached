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

use super::{parse_u32, ErrorKind, MetaValue, Response, Status, Value};

// TODO: remove this later in favour of individual parse_meta_*_status methods
#[allow(dead_code)]
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
        value(Response::Status(Status::Value), tag(b"VA ")),
        value(Response::Status(Status::Exists), tag(b"HD")),
        value(Response::Status(Status::NotFound), tag(b"EN")),
    ))(buf)
}

// TODO: refactor this into individual parse_meta_*_response methods
#[allow(dead_code)]
pub fn parse_meta_response(buf: &[u8]) -> Result<Option<(usize, Response)>, ErrorKind> {
    let bufn = buf.len();
    // TODO: alt calls parsers sequentially until one succeeds, which is not optimal.  Want to only call the correct parser, no sequencing.
    // Also run the risk of hitting the wrong parser since there is overlap in response codes.
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
    let (input, status) = parse_meta_get_status(buf)?; // removes <CD> response code from the input
    match status {
        // match arm for "VA " response when v flag is used
        Response::Status(Status::Value) => {
            let (input, size) = parse_u32(input)?; // parses the size of the data from the input
            let (input, meta_values_array) = parse_meta_flag_values_as_slice(input)?; // parses the flags from the input
            let (input, _) = crlf(input)?; // removes the leading crlf from the data block
            let (input, data) = take_until_size(input, size)?; // parses the data from the input

            let value = construct_value_from_meta_values(meta_values_array, Some(&data));

            Ok((input, Response::Data(Some(vec![value]))))
        }
        // match arm for "HD" response when v flag is omitted
        Response::Status(Status::Exists) => {
            // no value (data block) or size in this case, potentially just flags
            let (input, meta_values_array) = parse_meta_flag_values_as_slice(input)?;

            // early return if there were no flags passed in (no-op)
            if meta_values_array.is_empty() {
                return Ok((input, Response::Data(None)));
            }

            // data is empty in this case
            let data = None;
            let value = construct_value_from_meta_values(meta_values_array, data);

            Ok((input, Response::Data(Some(vec![value]))))
        }
        Response::Status(Status::NotFound) => {
            let (input, meta_values_array) = parse_meta_flag_values_as_slice(input)?;

            // return early if there were no flags passed in (miss without opaque or k flag)
            if meta_values_array.is_empty() {
                return Ok((input, Response::Status(Status::NotFound)));
            }

            // data is empty in this case
            let data = None;
            let value = construct_value_from_meta_values(meta_values_array, data);

            Ok((input, Response::Data(Some(vec![value]))))
        }
        _ => {
            // unexpected response code, should never happen, bail
            Err(nom::Err::Error(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Eof,
            )))
        }
    }
}

fn parse_meta_set_data_value(buf: &[u8]) -> IResult<&[u8], Response> {
    let (input, status) = parse_meta_set_status(buf)?;
    match status {
        Response::Status(Status::Stored) => {
            // no value (data block) or size in this case, potentially just flags
            let (input, meta_values_array) = parse_meta_flag_values_as_slice(input)?;

            // early return if there were no flags passed in
            if meta_values_array.is_empty() {
                return Ok((input, Response::Data(None)));
            }

            // data is empty in this case
            let data = None;
            let value = construct_value_from_meta_values(meta_values_array, data);

            Ok((input, Response::Data(Some(vec![value]))))
        }
        Response::Status(Status::NotStored) => {
            todo!("Handle Status::NotStored")
        }
        Response::Status(Status::Exists) => {
            todo!("Handle Status::Exists")
        }
        Response::Status(Status::NotFound) => {
            todo!("Handle Status::NotFound")
        }
        _ => Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Eof,
        ))),
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

#[allow(dead_code)]
fn parse_meta_flag_values_as_u32(input: &[u8]) -> IResult<&[u8], Vec<(u8, u32)>> {
    many0(pair(
        preceded(space0, map(take(1usize), |s: &[u8]| s[0])),
        map_res(digit1, |s: &[u8]| {
            std::str::from_utf8(s).unwrap().parse::<u32>()
        }),
    ))(input)
}

fn parse_meta_flag_values_as_slice(input: &[u8]) -> IResult<&[u8], Vec<(u8, &[u8])>> {
    if input.is_empty() || input == b"\r\n" {
        Ok((input, Vec::new()))
    } else {
        many0(map(
            tuple((
                space1,
                map(take(1usize), |s: &[u8]| s[0]),
                take_while1(|c: u8| c != b'\r' && c != b' '), // finding a space means more flags, finding \r means end of flags
            )),
            |(_, flag, value)| (flag, value),
        ))(input)
    }
}

fn map_meta_flag(flag: u8, meta_value: &[u8], value: &mut Value, meta_values: &mut MetaValue) {
    match flag {
        b'c' => {
            value.cas = Some(
                std::str::from_utf8(meta_value)
                    .expect("Failed to convert c flag to string")
                    .parse::<u64>()
                    .expect("Failed to convert c flag to u64"),
            )
        }
        b'f' => {
            value.flags = Some(
                std::str::from_utf8(meta_value)
                    .expect("Failed to convert f flag to string")
                    .parse::<u32>()
                    .expect("Failed to convert f flag to u32"),
            )
        }
        b'h' => {
            meta_values.hit_before = Some(meta_value != b"0");
        }
        b'k' => {
            value.key = meta_value.to_vec();
        }
        b'l' => {
            meta_values.last_accessed = Some(
                std::str::from_utf8(meta_value)
                    .expect("Failed to convert l flag to string")
                    .parse::<u64>()
                    .expect("Failed to convert l flag to u64"),
            )
        }
        b'O' => {
            meta_values.opaque_token = Some(meta_value.to_vec());
        }
        b's' => {
            meta_values.size = Some(
                std::str::from_utf8(meta_value)
                    .expect("Failed to convert s flag to string")
                    .parse::<u64>()
                    .expect("Failed to convert s flag to u64"),
            )
        }
        b't' => {
            meta_values.ttl_remaining = Some(
                std::str::from_utf8(meta_value)
                    .expect("Failed to convert t flag to string")
                    .parse::<i64>()
                    .expect("Failed to convert t flag to i64"),
            )
        }
        b'W' => meta_values.is_recache_winner = Some(true),
        b'Z' => meta_values.is_recache_winner = Some(false),
        b'X' => meta_values.is_stale = Some(true),
        _ => {}
    }
}

fn construct_value_from_meta_values(
    meta_values_array: Vec<(u8, &[u8])>,
    data: Option<&[u8]>,
) -> Value {
    let mut meta_values = MetaValue::default();

    let mut value = Value {
        key: Vec::new(),
        cas: None,
        flags: Some(0),
        data: data.map(|d| d.to_vec()),
        meta_values: None,
    };

    for (flag, meta_value) in meta_values_array {
        map_meta_flag(flag, meta_value, &mut value, &mut meta_values);
    }

    value.meta_values = Some(meta_values);

    value
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::parse_u32;
    use std::str;

    #[test]
    fn test_parse_meta_flag_values_as_slice_with_empty_input() {
        let empty_input = b"";
        let (remaining, result) = parse_meta_flag_values_as_slice(empty_input).unwrap();
        assert_eq!(result, vec![]);
        assert_eq!(remaining, b"");
    }

    #[test]
    fn test_parse_meta_flag_values_as_slice_with_crlf() {
        let new_line = b"\r\n";
        let (remaining, result) = parse_meta_flag_values_as_slice(new_line).unwrap();
        assert_eq!(result, vec![]);
        assert_eq!(remaining, b"\r\n");
    }

    #[test]
    fn test_parse_meta_flag_values_as_slice_with_single_flag() {
        let input = b" Oopaque-token\r\n";
        let (remaining, result) = parse_meta_flag_values_as_slice(input).unwrap();

        let token: &[u8] = "opaque-token".as_bytes();
        assert_eq!(result, vec![(b'O', token)]);
        assert_eq!(remaining, b"\r\n");
    }

    #[test]
    fn test_parse_meta_flag_values_as_slice_with_many_flags() {
        let input = b" h1 l1 t123 Oopaque-token\r\n";
        let (remaining, result) = parse_meta_flag_values_as_slice(input).unwrap();

        assert_eq!(
            result,
            vec![
                (b'h', b"1".as_ref()),
                (b'l', b"1".as_ref()),
                (b't', b"123".as_ref()),
                (b'O', b"opaque-token".as_ref())
            ]
        );
        assert_eq!(remaining, b"\r\n");
    }

    #[test]
    fn test_parse_meta_flag_values_as_slice_flag_order_does_not_matter() {
        let input = b" t2179 h1 l56\r\ntest-value\r\n";
        let (remaining, result) = parse_meta_flag_values_as_slice(input).unwrap();

        assert_eq!(
            result,
            vec![
                (b't', b"2179".as_ref()),
                (b'h', b"1".as_ref()),
                (b'l', b"56".as_ref()),
            ]
        );
        assert_eq!(remaining, b"\r\ntest-value\r\n");
    }

    #[test]
    fn test_parse_meta_flag_values_as_slice_handles_unknown_tags() {
        let input = b" h1 l56 t2179 x100\r\ntest-value\r\n";
        let (remaining, result) = parse_meta_flag_values_as_slice(input).unwrap();

        assert_eq!(
            result,
            vec![
                (b'h', b"1".as_ref()),
                (b'l', b"56".as_ref()),
                (b't', b"2179".as_ref()),
                (b'x', b"100".as_ref()),
            ]
        );
        assert_eq!(remaining, b"\r\ntest-value\r\n");
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
    fn test_parse_meta_get_data_value() {
        let input = b"VA 10 h1 l56 t2179 f9001\r\ntest-value\r\n";
        let (remaining, response) = parse_meta_get_data_value(input).unwrap();

        assert_eq!(remaining, b"");

        match response {
            Response::Data(Some(values)) => {
                assert_eq!(values.len(), 1);
                let value = &values[0];
                assert_eq!(
                    str::from_utf8(value.data.as_ref().unwrap()).unwrap(),
                    "test-value"
                );
                assert_eq!(value.flags, Some(9001));
                assert_eq!(value.cas, None);

                let meta_values = value.meta_values.as_ref().unwrap();
                assert_eq!(meta_values.hit_before, Some(true));
                assert_eq!(meta_values.last_accessed, Some(56));
                assert_eq!(meta_values.ttl_remaining, Some(2179));
            }
            _ => panic!("Expected Response::Data, got something else"),
        }
    }

    #[test]
    fn test_parse_meta_get_data_value_tag_order_does_not_matter() {
        let input = b"VA 10 t2179 h1 l56 Oopaque-token\r\ntest-value\r\n";
        let (remaining, response) = parse_meta_get_data_value(input).unwrap();

        assert_eq!(remaining, b"");

        match response {
            Response::Data(Some(values)) => {
                assert_eq!(values.len(), 1);
                let value = &values[0];
                assert_eq!(
                    str::from_utf8(value.data.as_ref().unwrap()).unwrap(),
                    "test-value"
                );
                assert_eq!(value.flags, Some(0));
                assert_eq!(value.cas, None);

                let meta_values = value.meta_values.as_ref().unwrap();
                assert_eq!(meta_values.hit_before, Some(true));
                assert_eq!(meta_values.last_accessed, Some(56));
                assert_eq!(meta_values.ttl_remaining, Some(2179));
                assert_eq!(meta_values.opaque_token, Some(b"opaque-token".to_vec()));
            }
            _ => panic!("Expected Response::Data, got something else"),
        }
    }

    #[test]
    fn test_parse_meta_get_data_value_with_only_v_flag() {
        let input = b"VA 10\r\ntest-value\r\n";
        let (remaining, response) = parse_meta_get_data_value(input).unwrap();

        assert_eq!(remaining, b"");

        match response {
            Response::Data(Some(values)) => {
                assert_eq!(values.len(), 1);
                let value = &values[0];
                assert_eq!(
                    str::from_utf8(value.data.as_ref().unwrap()).unwrap(),
                    "test-value"
                );
                assert_eq!(value.flags, Some(0));
                assert_eq!(value.cas, None);
            }
            _ => panic!("Expected Response::Data, got something else"),
        }
    }

    #[test]
    fn test_parse_meta_get_data_value_with_no_flags() {
        let input = b"HD\r\n";
        let (remaining, _) = parse_meta_get_data_value(input).unwrap();

        assert_eq!(remaining, b"\r\n");
    }

    #[test]
    fn test_parse_meta_get_data_value_with_only_non_v_flags() {
        let input = b"HD h1 c1 l123\r\n";
        let (remaining, response) = parse_meta_get_data_value(input).unwrap();

        assert_eq!(remaining, b"\r\n");

        match response {
            Response::Data(Some(values)) => {
                assert_eq!(values.len(), 1);
                let value = &values[0];
                assert_eq!(value.data, None);
                assert_eq!(value.flags, Some(0));
                assert_eq!(value.cas, Some(1));

                let meta_values = value.meta_values.as_ref().unwrap();
                assert_eq!(meta_values.hit_before, Some(true));
                assert_eq!(meta_values.last_accessed, Some(123));
            }
            _ => panic!("Expected Response::Data, got something else"),
        }
    }

    #[test]
    fn test_parse_meta_get_data_value_ignores_unknown_tags() {
        let input = b"VA 10 h1 l56 t2179 x100 Oopaque-token\r\ntest-value\r\n";
        let (remaining, response) = parse_meta_get_data_value(input).unwrap();

        assert_eq!(remaining, b"");

        match response {
            Response::Data(Some(values)) => {
                assert_eq!(values.len(), 1);
                let value = &values[0];
                assert_eq!(
                    str::from_utf8(value.data.as_ref().unwrap()).unwrap(),
                    "test-value"
                );
                assert_eq!(value.flags, Some(0));
                assert_eq!(value.cas, None);

                let meta_values = value.meta_values.as_ref().unwrap();
                assert_eq!(meta_values.hit_before, Some(true));
                assert_eq!(meta_values.last_accessed, Some(56));
                assert_eq!(meta_values.ttl_remaining, Some(2179));
                assert_eq!(meta_values.opaque_token, Some(b"opaque-token".to_vec()));
            }
            _ => panic!("Expected Response::Data, got something else"),
        }
    }

    #[test]
    fn test_parse_meta_get_data_value_maps_key_correctly_with_k_flag() {
        let input = b"VA 10 h1 l56 t2179 ktest-key\r\ntest-value\r\n";
        let (remaining, response) = parse_meta_get_data_value(input).unwrap();

        assert_eq!(remaining, b"");

        match response {
            Response::Data(Some(values)) => {
                assert_eq!(values.len(), 1);
                let value = &values[0];
                assert_eq!(value.key, b"test-key");
                assert_eq!(
                    str::from_utf8(value.data.as_ref().unwrap()).unwrap(),
                    "test-value"
                );
                assert_eq!(value.flags, Some(0));
                assert_eq!(value.cas, None);

                let meta_values = value.meta_values.as_ref().unwrap();
                assert_eq!(meta_values.hit_before, Some(true));
                assert_eq!(meta_values.last_accessed, Some(56));
                assert_eq!(meta_values.ttl_remaining, Some(2179));
            }
            _ => panic!("Expected Response::Data, got something else"),
        }
    }

    #[test]
    fn test_parse_meta_get_data_value_cache_miss() {
        let input = b"EN\r\n";
        let (remaining, response) = parse_meta_get_data_value(input).unwrap();
        assert_eq!(remaining, b"\r\n");
        assert_eq!(response, Response::Status(Status::NotFound));
    }

    #[test]
    fn test_parse_meta_get_data_value_cache_miss_with_opaque_token() {
        let input = b"EN Oopaque-token\r\n";
        let (remaining, response) = parse_meta_get_data_value(input).unwrap();

        assert_eq!(remaining, b"\r\n");

        match response {
            Response::Data(Some(values)) => {
                assert_eq!(values.len(), 1);
                let value = values.first().unwrap();
                assert_eq!(value.key, b"");
                assert_eq!(value.data, None);
                assert_eq!(value.flags, Some(0));
                assert_eq!(value.cas, None);

                let meta_values = value.meta_values.as_ref().unwrap();
                assert_eq!(meta_values.opaque_token, Some(b"opaque-token".to_vec()));
            }
            _ => panic!("Expected Response::Data, got something else"),
        }
    }

    #[test]
    fn test_parse_meta_get_data_value_cache_miss_with_k_flag() {
        let input = b"EN ktest-key\r\n";
        let (remaining, response) = parse_meta_get_data_value(input).unwrap();
        assert_eq!(remaining, b"\r\n");

        match response {
            Response::Data(Some(values)) => {
                assert_eq!(values.len(), 1);
                let value = values.first().unwrap();
                assert_eq!(value.key, b"test-key");
                assert_eq!(value.data, None);
                assert_eq!(value.flags, Some(0));
                assert_eq!(value.cas, None);

                let meta_values = value.meta_values.as_ref().unwrap();
                assert_eq!(*meta_values, MetaValue::default());
            }
            _ => panic!("Expected Response::Data, got something else"),
        }
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
                assert_eq!(
                    str::from_utf8(value.data.as_ref().unwrap()).unwrap(),
                    "test-\r\nvalue"
                );
                assert_eq!(value.flags, Some(0));
                assert_eq!(value.cas, None);

                let meta_values = value.meta_values.as_ref().unwrap();
                assert_eq!(meta_values.hit_before, Some(true));
                assert_eq!(meta_values.last_accessed, Some(56));
                assert_eq!(meta_values.ttl_remaining, Some(2179));
            }
            _ => panic!("Expected Response::Data, got something else"),
        }
    }
}
