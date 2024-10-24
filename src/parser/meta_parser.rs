use nom::{
    branch::alt,
    bytes::streaming::{tag, take, take_while1},
    character::streaming::{crlf, space1},
    combinator::{map, opt, value},
    error::ErrorKind::Fail,
    multi::many0,
    sequence::tuple,
    IResult,
};

use std::num::NonZero;

use super::{parse_u32, ErrorKind, MetaResponse, MetaValue, Status};
use crate::Error;

pub fn parse_meta_get_status(buf: &[u8]) -> IResult<&[u8], MetaResponse> {
    alt((
        value(MetaResponse::Status(Status::Value), tag(b"VA ")),
        value(MetaResponse::Status(Status::Exists), tag(b"HD")),
        value(MetaResponse::Status(Status::NotFound), tag(b"EN")),
        value(MetaResponse::Status(Status::NoOp), tag(b"MN\r\n")),
    ))(buf)
}

pub fn parse_meta_set_status(buf: &[u8]) -> IResult<&[u8], MetaResponse> {
    alt((
        value(MetaResponse::Status(Status::Stored), tag(b"HD")),
        value(MetaResponse::Status(Status::NotStored), tag(b"NS")),
        value(MetaResponse::Status(Status::Exists), tag(b"EX")),
        value(MetaResponse::Status(Status::NotFound), tag(b"NF")),
        value(MetaResponse::Status(Status::NoOp), tag(b"MN\r\n")),
    ))(buf)
}

pub fn parse_meta_delete_status(buf: &[u8]) -> IResult<&[u8], MetaResponse> {
    alt((
        value(MetaResponse::Status(Status::Deleted), tag(b"HD")),
        value(MetaResponse::Status(Status::NotFound), tag(b"NF")),
        value(MetaResponse::Status(Status::Exists), tag(b"EX")),
        value(MetaResponse::Status(Status::NoOp), tag(b"MN\r\n")),
    ))(buf)
}

pub fn parse_meta_get_response(buf: &[u8]) -> Result<Option<(usize, MetaResponse)>, ErrorKind> {
    let total_bytes = buf.len();
    let result = parse_meta_get_data_value(buf);

    match result {
        Ok((remaining_bytes, response)) => {
            let read_bytes = total_bytes - remaining_bytes.len();
            Ok(Some((read_bytes, response)))
        }
        Err(nom::Err::Incomplete(_)) => Ok(None),
        Err(nom::Err::Error(e)) | Err(nom::Err::Failure(e)) => {
            Err(ErrorKind::Protocol(Some(e.code.description().to_string())))
        }
    }
}

pub fn parse_meta_set_response(buf: &[u8]) -> Result<Option<(usize, MetaResponse)>, ErrorKind> {
    let total_bytes = buf.len();
    let result = parse_meta_set_data_value(buf);

    match result {
        Ok((remaining_bytes, response)) => {
            let read_bytes = total_bytes - remaining_bytes.len();
            Ok(Some((read_bytes, response)))
        }
        Err(nom::Err::Incomplete(_)) => Ok(None),
        Err(nom::Err::Error(e)) | Err(nom::Err::Failure(e)) => {
            Err(ErrorKind::Protocol(Some(e.code.description().to_string())))
        }
    }
}

pub fn parse_meta_delete_response(buf: &[u8]) -> Result<Option<(usize, MetaResponse)>, ErrorKind> {
    let total_bytes = buf.len();
    let result = parse_meta_delete_data_value(buf);

    match result {
        Ok((remaining_bytes, response)) => {
            let read_bytes = total_bytes - remaining_bytes.len();
            Ok(Some((read_bytes, response)))
        }
        Err(nom::Err::Incomplete(_)) => Ok(None),
        Err(nom::Err::Error(e)) | Err(nom::Err::Failure(e)) => {
            Err(ErrorKind::Protocol(Some(e.code.description().to_string())))
        }
    }
}

// example meta_get command sent to memcached server:
// mg meta-get-test-key v h l t k
// so it has flags of v h l t k
//
// meta_get example response from memcached server:
// VA 10 h1 l56 t2179
// test-value
//
// This method should return a response with a MetaValue object containing only the requested meta flag data, like this:
// MetaValue {
//     key: "meta-get-test-key",
//     cas: None,
//     flags: None,
//     data: [116, 101, 115, 116, 45, 118, 97, 108, 117, 101],
//     status: Some(Status::Value),
//     hit_before: Some(true),
//     last_accessed: Some(56),
//     ttl_remaining: Some(2179),
//     size: None,
//     opaque_token: None,
//     is_stale: None,
//     is_recache_winner: None,
// }
fn parse_meta_get_data_value(buf: &[u8]) -> IResult<&[u8], MetaResponse> {
    let (input, status) = parse_meta_get_status(buf)?; // removes <CD> response code from the input

    match status {
        // match arm for "VA " response when v flag is used
        MetaResponse::Status(Status::Value) => {
            let (input, size) = parse_u32(input)?; // parses the size of the data from the input
            let (input, flag_array) = parse_meta_flag_values_as_slice(input)?; // parses the flags from the input
            let (input, _) = crlf(input)?; // removes the leading crlf from the data block
            let (input, data) = take_until_size(input, size)?; // parses the data from the input

            let meta_value =
                construct_meta_value_from_flag_array(flag_array, Some(data), Some(Status::Value))
                    .map_err(|_| nom::Err::Failure(nom::error::Error::new(buf, Fail)))?; // Throw Fail as a generic nom error

            Ok((input, MetaResponse::Data(Some(vec![meta_value]))))
        }
        // match arm for "HD" response when v flag is omitted
        MetaResponse::Status(Status::Exists) => {
            // no value (data block) or size in this case, potentially just flags
            let (input, meta_values_array) = parse_meta_flag_values_as_slice(input)?;
            let (input, _) = crlf(input)?; // consume the trailing crlf and leave the buffer empty

            // early return if there were no flags passed in (no-op)
            if meta_values_array.is_empty() {
                return Ok((input, MetaResponse::Data(None)));
            }

            // data is empty in this case
            let meta_value =
                construct_meta_value_from_flag_array(meta_values_array, None, Some(Status::Exists))
                    .map_err(|_| nom::Err::Failure(nom::error::Error::new(buf, Fail)))?;

            Ok((input, MetaResponse::Data(Some(vec![meta_value]))))
        }
        // match arm for "EN" response
        MetaResponse::Status(Status::NotFound) => {
            process_meta_response_without_data_payload(input, Status::NotFound)
        }
        // match arm for "MN\r\n" response
        MetaResponse::Status(Status::NoOp) => Ok((input, MetaResponse::Status(Status::NoOp))),
        _ => {
            // unexpected response code, should never happen, bail
            Err(nom::Err::Error(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Eof,
            )))
        }
    }
}

// example meta_set command sent to memcached server:
// ms meta-set-test-key E9001 T3600 Oopaque-token c s\r\nTEST-VALUE\r\n
// so it has flags of E9001 T3600 Oopaque-token c s
//
// meta_set example response from memcached server:
// HD Oopaque-token c9001
//
// This method should return a response with a MetaValue object containing only the requested meta flag data, like this:
// MetaValue {
//     key: None,
//     cas: Some(9001),
//     flags: None,
//     data: None,
//     status: Some(Status::Stored),
//     hit_before: None,
//     last_accessed: None,
//     ttl_remaining: None,
//     size: 10,
//     opaque_token: Some(opaque-token),
//     is_stale: None,
//     is_recache_winner: None,
// }
fn parse_meta_set_data_value(buf: &[u8]) -> IResult<&[u8], MetaResponse> {
    let (input, status) = parse_meta_set_status(buf)?;

    match status {
        // match arm for "MN\r\n" response
        MetaResponse::Status(Status::NoOp) => Ok((input, MetaResponse::Status(Status::NoOp))),
        // match arm for "HD", "NS", "EX" & "NF" responses
        MetaResponse::Status(s) => process_meta_response_without_data_payload(input, s),
        _ => Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Eof,
        ))),
    }
}

fn parse_meta_delete_data_value(buf: &[u8]) -> IResult<&[u8], MetaResponse> {
    let (input, status) = parse_meta_delete_status(buf)?; // removes <CD> response code from the input

    match status {
        // match arm for "MN\r\n" response
        MetaResponse::Status(Status::NoOp) => Ok((input, MetaResponse::Status(Status::NoOp))),
        // match arm for "HD", "NF" & "EX" responses
        MetaResponse::Status(s) => process_meta_response_without_data_payload(input, s),
        _ => Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Eof,
        ))),
    }
}

pub fn take_until_size(buf: &[u8], byte_size: u32) -> IResult<&[u8], &[u8]> {
    let size = byte_size as usize;

    // Check if the buffer has enough bytes for the data and the trailing "\r\n"
    if buf.len() < size {
        // Not enough data yet; request more
        return Err(nom::Err::Incomplete(nom::Needed::Size(
            NonZero::new(size).unwrap(),
        )));
    }

    // Slice the buffer to extract the data
    let (extracted, remaining) = buf.split_at(size);

    // Ensure the remaining buffer starts with "\r\n"
    let (remaining, _) = tag("\r\n")(remaining)?;

    Ok((remaining, extracted))
}

#[allow(clippy::type_complexity)]
fn parse_meta_flag_values_as_slice(input: &[u8]) -> IResult<&[u8], Vec<(u8, Option<&[u8]>)>> {
    if input.is_empty() || input == b"\r\n" {
        Ok((input, Vec::new()))
    } else {
        many0(map(
            tuple((
                space1,
                map(take(1usize), |s: &[u8]| s[0]),
                opt(take_while1(|c: u8| c != b'\r' && c != b' ')), // finding a space means more flags, finding \r means end of flags
            )),
            |(_, flag, value)| (flag, value),
        ))(input)
    }
}

fn map_meta_flag(flag: u8, token: &[u8], meta_value: &mut MetaValue) -> Result<(), crate::Error> {
    match flag {
        b'c' => {
            meta_value.cas = Some(
                std::str::from_utf8(token)
                    .map_err(|_| {
                        Error::Protocol(Status::Error(ErrorKind::Generic(
                            "Failed to parse c flag".to_string(),
                        )))
                    })?
                    .parse::<u64>()
                    .map_err(|_| {
                        Error::Protocol(Status::Error(ErrorKind::Generic(
                            "Invalid CAS value".to_string(),
                        )))
                    })?,
            );
        }
        b'f' => {
            meta_value.flags = Some(
                std::str::from_utf8(token)
                    .map_err(|_| {
                        Error::Protocol(Status::Error(ErrorKind::Generic(
                            "Failed to parse f flag".to_string(),
                        )))
                    })?
                    .parse::<u32>()
                    .map_err(|_| {
                        Error::Protocol(Status::Error(ErrorKind::Generic(
                            "Invalid client flags value".to_string(),
                        )))
                    })?,
            )
        }
        b'h' => {
            meta_value.hit_before = Some(token != b"0");
        }
        b'k' => {
            meta_value.key = Some(token.to_vec());
        }
        b'l' => {
            meta_value.last_accessed = Some(
                std::str::from_utf8(token)
                    .map_err(|_| {
                        Error::Protocol(Status::Error(ErrorKind::Generic(
                            "Failed to parse l flag".to_string(),
                        )))
                    })?
                    .parse::<u64>()
                    .map_err(|_| {
                        Error::Protocol(Status::Error(ErrorKind::Generic(
                            "Invalid last accessed value".to_string(),
                        )))
                    })?,
            )
        }
        b'O' => {
            meta_value.opaque_token = Some(token.to_vec());
        }
        b's' => {
            meta_value.size = Some(
                std::str::from_utf8(token)
                    .map_err(|_| {
                        Error::Protocol(Status::Error(ErrorKind::Generic(
                            "Failed to parse s flag".to_string(),
                        )))
                    })?
                    .parse::<u64>()
                    .map_err(|_| {
                        Error::Protocol(Status::Error(ErrorKind::Generic(
                            "Invalid size value".to_string(),
                        )))
                    })?,
            )
        }
        b't' => {
            meta_value.ttl_remaining = Some(
                std::str::from_utf8(token)
                    .map_err(|_| {
                        Error::Protocol(Status::Error(ErrorKind::Generic(
                            "Failed to parse t flag".to_string(),
                        )))
                    })?
                    .parse::<i64>()
                    .map_err(|_| {
                        Error::Protocol(Status::Error(ErrorKind::Generic(
                            "Invalid ttl remaining value".to_string(),
                        )))
                    })?,
            )
        }
        b'W' => meta_value.is_recache_winner = Some(true),
        b'Z' => meta_value.is_recache_winner = Some(false),
        b'X' => meta_value.is_stale = Some(true),
        _ => {}
    }

    Ok(())
}

fn construct_meta_value_from_flag_array(
    flag_array: Vec<(u8, Option<&[u8]>)>,
    data: Option<&[u8]>,
    status: Option<Status>,
) -> Result<MetaValue, Error> {
    // might need something like this in the case of tombstoned data.
    // Currently returns Some([]), but should maybe be converted to None?
    // let data = if let Some(d) = data {
    //     if d.is_empty() {
    //         None
    //     } else {
    //         Some(d.to_vec())
    //     }
    // } else {
    //     None
    // };

    let mut meta_value = MetaValue {
        status,
        data: data.map(|d| d.to_vec()),
        ..Default::default()
    };

    let flag_array: Vec<(u8, &[u8])> = flag_array
        .iter()
        .map(|(flag, value)| (*flag, value.unwrap_or_default()))
        .collect();

    for (flag, token) in flag_array {
        map_meta_flag(flag, token, &mut meta_value)?;
    }

    Ok(meta_value)
}

fn process_meta_response_without_data_payload(
    input: &[u8],
    status: Status,
) -> IResult<&[u8], MetaResponse> {
    // no value (data block) or size in this case, potentially just flags
    let (input, meta_values_array) = parse_meta_flag_values_as_slice(input)?;
    let (input, _) = crlf(input)?; // consume the trailing crlf and leave the buffer empty

    // early return if there were no flags passed in
    if meta_values_array.is_empty() {
        return Ok((input, MetaResponse::Status(status)));
    }

    // data is empty in this case
    let meta_value = construct_meta_value_from_flag_array(meta_values_array, None, Some(status))
        .map_err(|_| nom::Err::Failure(nom::error::Error::new(input, Fail)))?;

    Ok((input, MetaResponse::Data(Some(vec![meta_value]))))
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
        assert_eq!(result, vec![(b'O', Some(token))]);
        assert_eq!(remaining, b"\r\n");
    }

    #[test]
    fn test_parse_meta_flag_values_as_slice_with_many_flags() {
        let input = b" h1 l1 t123 Oopaque-token\r\n";
        let (remaining, result) = parse_meta_flag_values_as_slice(input).unwrap();

        assert_eq!(
            result,
            vec![
                (b'h', Some(b"1".as_ref())),
                (b'l', Some(b"1".as_ref())),
                (b't', Some(b"123".as_ref())),
                (b'O', Some(b"opaque-token".as_ref()))
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
                (b't', Some(b"2179".as_ref())),
                (b'h', Some(b"1".as_ref())),
                (b'l', Some(b"56".as_ref())),
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
                (b'h', Some(b"1".as_ref())),
                (b'l', Some(b"56".as_ref())),
                (b't', Some(b"2179".as_ref())),
                (b'x', Some(b"100".as_ref())),
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
            MetaResponse::Data(Some(meta_values)) => {
                assert_eq!(meta_values.len(), 1);
                let meta_value = &meta_values[0];
                assert_eq!(
                    str::from_utf8(meta_value.data.as_ref().unwrap()).unwrap(),
                    "test-value"
                );
                assert_eq!(meta_value.flags, Some(9001));
                assert_eq!(meta_value.cas, None);
                assert_eq!(meta_value.hit_before, Some(true));
                assert_eq!(meta_value.last_accessed, Some(56));
                assert_eq!(meta_value.ttl_remaining, Some(2179));
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
            MetaResponse::Data(Some(meta_values)) => {
                assert_eq!(meta_values.len(), 1);
                let meta_value = &meta_values[0];
                assert_eq!(
                    str::from_utf8(meta_value.data.as_ref().unwrap()).unwrap(),
                    "test-value"
                );
                assert_eq!(meta_value.flags, None);
                assert_eq!(meta_value.cas, None);
                assert_eq!(meta_value.hit_before, Some(true));
                assert_eq!(meta_value.last_accessed, Some(56));
                assert_eq!(meta_value.ttl_remaining, Some(2179));
                assert_eq!(meta_value.opaque_token, Some(b"opaque-token".to_vec()));
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
            MetaResponse::Data(Some(meta_values)) => {
                assert_eq!(meta_values.len(), 1);
                let meta_value = &meta_values[0];
                assert_eq!(
                    str::from_utf8(meta_value.data.as_ref().unwrap()).unwrap(),
                    "test-value"
                );
                assert_eq!(meta_value.flags, None);
                assert_eq!(meta_value.cas, None);
            }
            _ => panic!("Expected Response::Data, got something else"),
        }
    }

    #[test]
    fn test_parse_meta_get_data_value_with_no_flags() {
        let input = b"HD\r\n";
        let (remaining, response) = parse_meta_get_data_value(input).unwrap();

        assert_eq!(remaining, b"");

        assert_eq!(response, MetaResponse::Data(None));
    }

    #[test]
    fn test_parse_meta_get_data_value_with_only_non_v_flags() {
        let input = b"HD h1 c1 l123\r\n";
        let (remaining, response) = parse_meta_get_data_value(input).unwrap();

        assert_eq!(remaining, b"");

        match response {
            MetaResponse::Data(Some(meta_values)) => {
                assert_eq!(meta_values.len(), 1);
                let meta_value = &meta_values[0];
                assert_eq!(meta_value.data, None);
                assert_eq!(meta_value.flags, None);
                assert_eq!(meta_value.cas, Some(1));
                assert_eq!(meta_value.hit_before, Some(true));
                assert_eq!(meta_value.last_accessed, Some(123));
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
            MetaResponse::Data(Some(meta_values)) => {
                assert_eq!(meta_values.len(), 1);
                let meta_value = &meta_values[0];
                assert_eq!(
                    str::from_utf8(meta_value.data.as_ref().unwrap()).unwrap(),
                    "test-value"
                );
                assert_eq!(meta_value.flags, None);
                assert_eq!(meta_value.cas, None);
                assert_eq!(meta_value.hit_before, Some(true));
                assert_eq!(meta_value.last_accessed, Some(56));
                assert_eq!(meta_value.ttl_remaining, Some(2179));
                assert_eq!(meta_value.opaque_token, Some(b"opaque-token".to_vec()));
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
            MetaResponse::Data(Some(meta_values)) => {
                assert_eq!(meta_values.len(), 1);
                let meta_value = &meta_values[0];
                assert_eq!(meta_value.key, Some(b"test-key".to_vec()));
                assert_eq!(
                    str::from_utf8(meta_value.data.as_ref().unwrap()).unwrap(),
                    "test-value"
                );
                assert_eq!(meta_value.flags, None);
                assert_eq!(meta_value.cas, None);
                assert_eq!(meta_value.hit_before, Some(true));
                assert_eq!(meta_value.last_accessed, Some(56));
                assert_eq!(meta_value.ttl_remaining, Some(2179));
            }
            _ => panic!("Expected Response::Data, got something else"),
        }
    }

    #[test]
    fn test_parse_meta_get_data_value_cache_miss() {
        let input = b"EN\r\n";
        let (remaining, response) = parse_meta_get_data_value(input).unwrap();
        assert_eq!(remaining, b"");
        assert_eq!(response, MetaResponse::Status(Status::NotFound));
    }

    #[test]
    fn test_parse_meta_get_data_value_cache_miss_with_opaque_token() {
        let input = b"EN Oopaque-token\r\n";
        let (remaining, response) = parse_meta_get_data_value(input).unwrap();

        assert_eq!(remaining, b"");

        match response {
            MetaResponse::Data(Some(meta_values)) => {
                assert_eq!(meta_values.len(), 1);
                let meta_value = &meta_values[0];
                assert_eq!(meta_value.key, None);
                assert_eq!(meta_value.data, None);
                assert_eq!(meta_value.flags, None);
                assert_eq!(meta_value.cas, None);
                assert_eq!(meta_value.opaque_token, Some(b"opaque-token".to_vec()));
            }
            _ => panic!("Expected Response::Data, got something else"),
        }
    }

    #[test]
    fn test_parse_meta_get_data_value_cache_miss_with_k_flag() {
        let input = b"EN ktest-key\r\n";
        let (remaining, response) = parse_meta_get_data_value(input).unwrap();
        assert_eq!(remaining, b"");

        match response {
            MetaResponse::Data(Some(meta_values)) => {
                assert_eq!(meta_values.len(), 1);
                let meta_value = &meta_values[0];
                assert_eq!(meta_value.key, Some(b"test-key".to_vec()));
                assert_eq!(meta_value.data, None);
                assert_eq!(meta_value.flags, None);
                assert_eq!(meta_value.cas, None);
                assert_eq!(meta_value.status, Some(Status::NotFound));
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
        // full crlf in value
        let input = b"VA 12 h1 l56 t2179\r\ntest-\r\nvalue\r\n";
        let (remaining, response) = parse_meta_get_data_value(input).unwrap();

        assert_eq!(
            remaining,
            b"",
            "Remaining input should be empty but was {:?}",
            str::from_utf8(remaining).unwrap()
        );
        match response {
            MetaResponse::Data(Some(meta_values)) => {
                assert_eq!(meta_values.len(), 1);
                let meta_value = &meta_values[0];
                assert_eq!(
                    str::from_utf8(meta_value.data.as_ref().unwrap()).unwrap(),
                    "test-\r\nvalue"
                );
                assert_eq!(meta_value.flags, None);
                assert_eq!(meta_value.cas, None);
                assert_eq!(meta_value.hit_before, Some(true));
                assert_eq!(meta_value.last_accessed, Some(56));
                assert_eq!(meta_value.ttl_remaining, Some(2179));
            }
            _ => panic!("Expected Response::Data, got something else"),
        }
    }

    #[test]
    fn test_parse_meta_get_data_value_handles_data_with_embedded_cr_followed_by_nl() {
        // \r followed by \n in value
        let input = b"VA 12 h1 l56 t2179\r\ntest\r-\nvalue\r\n";
        let (remaining, response) = parse_meta_get_data_value(input).unwrap();

        assert_eq!(
            remaining,
            b"",
            "Remaining input should be empty but was {:?}",
            str::from_utf8(remaining).unwrap()
        );
        match response {
            MetaResponse::Data(Some(meta_values)) => {
                assert_eq!(meta_values.len(), 1);
                let meta_value = &meta_values[0];
                assert_eq!(
                    str::from_utf8(meta_value.data.as_ref().unwrap()).unwrap(),
                    "test\r-\nvalue"
                );
                assert_eq!(meta_value.flags, None);
                assert_eq!(meta_value.cas, None);
                assert_eq!(meta_value.hit_before, Some(true));
                assert_eq!(meta_value.last_accessed, Some(56));
                assert_eq!(meta_value.ttl_remaining, Some(2179));
            }
            _ => panic!("Expected Response::Data, got something else"),
        }
    }

    #[test]
    fn test_parse_meta_get_data_value_handles_data_with_embedded_nl_followed_by_cr() {
        // \n followed by \r in value
        let input = b"VA 12 h1 l56 t2179\r\ntest\n-\rvalue\r\n";
        let (remaining, response) = parse_meta_get_data_value(input).unwrap();

        assert_eq!(
            remaining,
            b"",
            "Remaining input should be empty but was {:?}",
            str::from_utf8(remaining).unwrap()
        );
        match response {
            MetaResponse::Data(Some(meta_values)) => {
                assert_eq!(meta_values.len(), 1);
                let meta_value = &meta_values[0];
                assert_eq!(
                    str::from_utf8(meta_value.data.as_ref().unwrap()).unwrap(),
                    "test\n-\rvalue"
                );
                assert_eq!(meta_value.flags, None);
                assert_eq!(meta_value.cas, None);
                assert_eq!(meta_value.hit_before, Some(true));
                assert_eq!(meta_value.last_accessed, Some(56));
                assert_eq!(meta_value.ttl_remaining, Some(2179));
            }
            _ => panic!("Expected Response::Data, got something else"),
        }
    }

    #[test]
    fn test_parse_meta_set_data_value_stored_with_no_flags() {
        let server_response = b"HD\r\n";
        let (remaining, response) = parse_meta_set_data_value(server_response).unwrap();

        assert_eq!(remaining, b"");

        assert_eq!(response, MetaResponse::Status(Status::Stored));
    }

    #[test]
    fn test_parse_meta_set_data_value_not_stored_with_no_flags() {
        let server_response = b"NS\r\n";
        let (remaining, response) = parse_meta_set_data_value(server_response).unwrap();

        assert_eq!(remaining, b"");

        assert_eq!(response, MetaResponse::Status(Status::NotStored));
    }

    #[test]
    fn test_parse_meta_set_data_value_exists_with_no_flags() {
        let server_response = b"EX\r\n";
        let (remaining, response) = parse_meta_set_data_value(server_response).unwrap();

        assert_eq!(remaining, b"");

        assert_eq!(response, MetaResponse::Status(Status::Exists));
    }

    #[test]
    fn test_parse_meta_set_data_value_not_found_with_no_flags() {
        let server_response = b"NF\r\n";
        let (remaining, response) = parse_meta_set_data_value(server_response).unwrap();

        assert_eq!(remaining, b"");

        assert_eq!(response, MetaResponse::Status(Status::NotFound));
    }

    #[test]
    fn test_parse_meta_set_data_value_success_with_opaque_token() {
        let server_response = b"HD O123\r\n";
        let (remaining, response) = parse_meta_set_data_value(server_response).unwrap();

        assert_eq!(remaining, b"");

        match response {
            MetaResponse::Data(Some(meta_values)) => {
                assert_eq!(meta_values.len(), 1);
                let meta_value = &meta_values[0];
                assert_eq!(meta_value.data, None);
                assert_eq!(meta_value.flags, None);
                assert_eq!(meta_value.cas, None);
                assert_eq!(meta_value.opaque_token, Some(b"123".to_vec()));
            }
            _ => panic!("Expected Response::Data, got something else"),
        }
    }

    #[test]
    fn test_parse_meta_set_data_value_success_with_k_flag() {
        let server_response = b"HD ktest-key\r\n";
        let (remaining, response) = parse_meta_set_data_value(server_response).unwrap();

        assert_eq!(remaining, b"");

        match response {
            MetaResponse::Data(Some(meta_values)) => {
                assert_eq!(meta_values.len(), 1);
                let meta_value = &meta_values[0];
                assert_eq!(meta_value.key, Some(b"test-key".to_vec()));
                assert_eq!(meta_value.data, None);
                assert_eq!(meta_value.flags, None);
                assert_eq!(meta_value.cas, None);
                assert_eq!(meta_value.status, Some(Status::Stored));
            }
            _ => panic!("Expected Response::Data, got something else"),
        }
    }
}
