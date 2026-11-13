//! Property tests for the response parsers.
//!
//! Each generated reply must parse to the value it encodes and consume exactly its own bytes.
//! Each proper prefix of a reply must parse as incomplete, because the client reads replies that
//! arrive split at any byte.

use super::{
    parse_ascii_metadump_response, parse_ascii_response, parse_ascii_stats_response,
    parse_meta_arithmetic_response, parse_meta_delete_response, parse_meta_get_response,
    parse_meta_set_response, ErrorKind, KeyMetadata, MetaResponse, MetaValue, MetadumpResponse,
    Response, StatsResponse, Status, Value,
};
use proptest::collection::vec;
use proptest::prelude::*;
use proptest::sample::select;
use std::fmt::Debug;

type Parse<T> = fn(&[u8]) -> Result<Option<(usize, T)>, ErrorKind>;

/// Byte sequences that look like reply syntax. Values and fuzz input contain them, so a parser
/// that searches for a terminator instead of counting the declared length misreads them.
const REPLY_FRAGMENTS: &[&[u8]] = &[
    b"\r\n",
    b"\r",
    b"\n",
    b" ",
    b"0",
    b"-1",
    b"END\r\n",
    b"VALUE key 0 1\r\n",
    b"STORED\r\n",
    b"SERVER_ERROR x\r\n",
    b"STAT a b\r\n",
    b"key=a exp=-1 la=1 cas=1 fetch=no cls=1 size=1\n",
    b"VA 1 kkey\r\n",
    b"HD",
    b"EN",
    b"NF",
    b"MN\r\n",
    b" c1",
    b" t-1",
];

/// Checks that `frame` parses to `reply` as one complete reply, and that each proper prefix of
/// `frame` parses as incomplete.
fn assert_frame<T: Debug + PartialEq>(
    parse: Parse<T>,
    frame: &[u8],
    reply: T,
) -> Result<(), TestCaseError> {
    let shown = frame.escape_ascii();
    prop_assert_eq!(
        parse(frame),
        Ok(Some((frame.len(), reply))),
        "frame {}",
        shown
    );
    for end in 0..frame.len() {
        let prefix = frame[..end].escape_ascii();
        prop_assert_eq!(
            parse(&frame[..end]),
            Ok(None),
            "prefix {} of {}",
            prefix,
            shown
        );
    }
    Ok(())
}

/// Checks that consecutive replies in one buffer parse one at a time, each from its own bytes.
fn assert_sequence<T: Debug + PartialEq>(
    parse: Parse<T>,
    replies: Vec<(Vec<u8>, T)>,
) -> Result<(), TestCaseError> {
    let bytes = replies
        .iter()
        .flat_map(|(frame, _)| frame.iter().copied())
        .collect::<Vec<u8>>();
    let mut rest = &bytes[..];
    for (frame, reply) in replies {
        let shown = rest.escape_ascii();
        prop_assert_eq!(
            parse(rest),
            Ok(Some((frame.len(), reply))),
            "input {}",
            shown
        );
        rest = &rest[frame.len()..];
    }
    Ok(())
}

/// Checks the result for arbitrary bytes: incomplete, a protocol error, or one reply that is a
/// prefix of `bytes` and parses the same way on its own.
fn assert_bounded<T: Debug + PartialEq>(
    parse: Parse<T>,
    bytes: &[u8],
) -> Result<(), TestCaseError> {
    let result = parse(bytes);
    prop_assert!(
        matches!(result, Ok(_) | Err(ErrorKind::Protocol(_))),
        "{:?} for {}",
        result,
        bytes.escape_ascii()
    );
    if let Ok(Some((read, reply))) = result {
        prop_assert!(
            read > 0 && read <= bytes.len(),
            "read {} of {}",
            read,
            bytes.escape_ascii()
        );
        assert_frame(parse, &bytes[..read], reply)?;
    }
    Ok(())
}

/// Keys made of visible ASCII characters.
fn key() -> impl Strategy<Value = Vec<u8>> {
    prop_oneof![
        4 => vec(0x21u8..=0x7e, 1..=16),
        1 => vec(0x21u8..=0x7e, 1..=250),
    ]
}

/// Printable ASCII text, as in server error messages.
fn text() -> impl Strategy<Value = String> {
    vec(0x20u8..=0x7e, 0..=24).prop_map(|bytes| String::from_utf8(bytes).unwrap())
}

/// Stats names and values.
fn token() -> impl Strategy<Value = String> {
    vec(0x21u8..=0x7e, 1..=24).prop_map(|bytes| String::from_utf8(bytes).unwrap())
}

fn mixed_bytes(max_parts: usize) -> impl Strategy<Value = Vec<u8>> {
    vec(
        prop_oneof![
            2 => any::<u8>().prop_map(|byte| vec![byte]),
            1 => select(REPLY_FRAGMENTS).prop_map(<[u8]>::to_vec),
        ],
        0..=max_parts,
    )
    .prop_map(|parts| parts.concat())
}

/// Value bytes.
fn payload() -> impl Strategy<Value = Vec<u8>> {
    mixed_bytes(12)
}

/// Input for the parsers that is not always a valid reply.
fn wire_bytes() -> impl Strategy<Value = Vec<u8>> {
    mixed_bytes(24)
}

fn ascii_value() -> impl Strategy<Value = Value> {
    (key(), any::<u32>(), any::<Option<u64>>(), payload()).prop_map(|(key, flags, cas, data)| {
        Value {
            key,
            cas,
            flags: Some(flags),
            data: Some(data),
        }
    })
}

fn value_block(values: Vec<Value>) -> (Vec<u8>, Response) {
    let mut frame = Vec::new();
    for value in &values {
        let data = value.data.as_deref().unwrap_or_default();
        frame.extend_from_slice(b"VALUE ");
        frame.extend_from_slice(&value.key);
        frame.extend_from_slice(
            format!(" {} {}", value.flags.unwrap_or_default(), data.len()).as_bytes(),
        );
        if let Some(cas) = value.cas {
            frame.extend_from_slice(format!(" {cas}").as_bytes());
        }
        frame.extend_from_slice(b"\r\n");
        frame.extend_from_slice(data);
        frame.extend_from_slice(b"\r\n");
    }
    frame.extend_from_slice(b"END\r\n");
    let reply = Response::Data((!values.is_empty()).then_some(values));
    (frame, reply)
}

/// One-line ASCII replies: statuses, server errors and counter values.
fn ascii_line() -> impl Strategy<Value = (Vec<u8>, Response)> {
    let status = select(vec![
        ("STORED", Status::Stored),
        ("NOT_STORED", Status::NotStored),
        ("DELETED", Status::Deleted),
        ("TOUCHED", Status::Touched),
        ("EXISTS", Status::Exists),
        ("NOT_FOUND", Status::NotFound),
        ("ERROR", Status::Error(ErrorKind::NonexistentCommand)),
    ])
    .prop_map(|(line, status)| (format!("{line}\r\n").into_bytes(), Response::Status(status)));
    let server_error = text().prop_map(|message| {
        (
            format!("SERVER_ERROR {message}\r\n").into_bytes(),
            Response::Status(Status::Error(ErrorKind::Server(message))),
        )
    });
    let counter = any::<u64>().prop_map(|count| {
        (
            format!("{count}\r\n").into_bytes(),
            Response::IncrDecr(count),
        )
    });
    prop_oneof![status, server_error, counter]
}

fn ascii_reply() -> impl Strategy<Value = (Vec<u8>, Response)> {
    prop_oneof![
        ascii_line(),
        vec(ascii_value(), 0..=2).prop_map(value_block)
    ]
}

fn metadump_reply() -> impl Strategy<Value = (Vec<u8>, MetadumpResponse)> {
    let entry = (
        key(),
        any::<i64>(),
        any::<u64>(),
        any::<u64>(),
        any::<bool>(),
        any::<u32>(),
        any::<u32>(),
        prop::option::of(text()),
        any::<bool>(),
    )
        .prop_map(
            |(key, expiration, last_accessed, cas, fetched, class_id, size, extra, newline)| {
                let mut line = if newline { b"\n".to_vec() } else { Vec::new() };
                line.extend_from_slice(b"key=");
                line.extend_from_slice(&key);
                let fetch = if fetched { "yes" } else { "no" };
                line.extend_from_slice(
                    format!(
                        " exp={expiration} la={last_accessed} cas={cas} fetch={fetch} \
                         cls={class_id} size={size}"
                    )
                    .as_bytes(),
                );
                // Newer servers add fields after `size`.
                if let Some(extra) = extra {
                    line.push(b' ');
                    line.extend_from_slice(extra.as_bytes());
                }
                line.push(b'\n');
                let entry = KeyMetadata {
                    key,
                    expiration,
                    last_accessed,
                    cas,
                    fetched,
                    class_id,
                    size,
                };
                (line, MetadumpResponse::Entry(entry))
            },
        );
    let refusal = (any::<bool>(), text()).prop_map(|(busy, message)| {
        if busy {
            (
                format!("BUSY {message}\r\n").into_bytes(),
                MetadumpResponse::Busy(message),
            )
        } else {
            (
                format!("BADCLASS {message}\r\n").into_bytes(),
                MetadumpResponse::BadClass(message),
            )
        }
    });
    prop_oneof![
        4 => entry,
        1 => refusal,
        1 => Just((b"END\r\n".to_vec(), MetadumpResponse::End)),
    ]
}

fn stats_reply() -> impl Strategy<Value = (Vec<u8>, StatsResponse)> {
    prop_oneof![
        4 => (token(), token()).prop_map(|(name, value)| {
            (
                format!("STAT {name} {value}\r\n").into_bytes(),
                StatsResponse::Entry(name, value),
            )
        }),
        1 => Just((b"END\r\n".to_vec(), StatsResponse::End)),
    ]
}

#[derive(Clone, Debug)]
enum MetaFlag {
    Cas(u64),
    ClientFlags(u32),
    Hit(bool),
    Key(Vec<u8>),
    LastAccess(u64),
    Opaque(Vec<u8>),
    Size(u64),
    Ttl(i64),
    RecacheWinner(bool),
    Stale,
    Unknown(u8, Vec<u8>),
}

impl MetaFlag {
    /// The `MetaValue` field the flag sets, so a reply carries at most one flag per field.
    fn field(&self) -> u8 {
        match self {
            Self::Cas(_) => b'c',
            Self::ClientFlags(_) => b'f',
            Self::Hit(_) => b'h',
            Self::Key(_) => b'k',
            Self::LastAccess(_) => b'l',
            Self::Opaque(_) => b'O',
            Self::Size(_) => b's',
            Self::Ttl(_) => b't',
            Self::RecacheWinner(_) => b'W',
            Self::Stale => b'X',
            Self::Unknown(letter, _) => *letter,
        }
    }

    fn encode(&self, frame: &mut Vec<u8>) {
        frame.push(b' ');
        match self {
            Self::Cas(cas) => frame.extend_from_slice(format!("c{cas}").as_bytes()),
            Self::ClientFlags(flags) => frame.extend_from_slice(format!("f{flags}").as_bytes()),
            Self::Hit(hit) => frame.extend_from_slice(if *hit { b"h1" } else { b"h0" }),
            Self::Key(key) => {
                frame.push(b'k');
                frame.extend_from_slice(key);
            }
            Self::LastAccess(seconds) => frame.extend_from_slice(format!("l{seconds}").as_bytes()),
            Self::Opaque(opaque) => {
                frame.push(b'O');
                frame.extend_from_slice(opaque);
            }
            Self::Size(size) => frame.extend_from_slice(format!("s{size}").as_bytes()),
            Self::Ttl(ttl) => frame.extend_from_slice(format!("t{ttl}").as_bytes()),
            Self::RecacheWinner(won) => frame.push(if *won { b'W' } else { b'Z' }),
            Self::Stale => frame.push(b'X'),
            Self::Unknown(letter, token) => {
                frame.push(*letter);
                frame.extend_from_slice(token);
            }
        }
    }

    fn apply(&self, value: &mut MetaValue) {
        match self {
            Self::Cas(cas) => value.cas = Some(*cas),
            Self::ClientFlags(flags) => value.flags = Some(*flags),
            Self::Hit(hit) => value.hit_before = Some(*hit),
            Self::Key(key) => value.key = Some(key.clone()),
            Self::LastAccess(seconds) => value.last_accessed = Some(*seconds),
            Self::Opaque(opaque) => value.opaque_token = Some(opaque.clone()),
            Self::Size(size) => value.size = Some(*size),
            Self::Ttl(ttl) => value.ttl_remaining = Some(*ttl),
            Self::RecacheWinner(won) => value.is_recache_winner = Some(*won),
            Self::Stale => value.is_stale = Some(true),
            Self::Unknown(..) => {}
        }
    }
}

fn meta_flag() -> impl Strategy<Value = MetaFlag> {
    prop_oneof![
        any::<u64>().prop_map(MetaFlag::Cas),
        any::<u32>().prop_map(MetaFlag::ClientFlags),
        any::<bool>().prop_map(MetaFlag::Hit),
        key().prop_map(MetaFlag::Key),
        any::<u64>().prop_map(MetaFlag::LastAccess),
        vec(0x21u8..=0x7e, 1..=32).prop_map(MetaFlag::Opaque),
        any::<u64>().prop_map(MetaFlag::Size),
        any::<i64>().prop_map(MetaFlag::Ttl),
        any::<bool>().prop_map(MetaFlag::RecacheWinner),
        Just(MetaFlag::Stale),
        (select(&b"aNx"[..]), vec(0x21u8..=0x7e, 0..=8))
            .prop_map(|(letter, token)| MetaFlag::Unknown(letter, token)),
    ]
}

/// Return flags in any order, with at most one flag per field.
fn meta_flags() -> impl Strategy<Value = Vec<MetaFlag>> {
    vec(meta_flag(), 0..=6).prop_map(|candidates| {
        let mut flags: Vec<MetaFlag> = Vec::new();
        for flag in candidates {
            if flags.iter().all(|kept| kept.field() != flag.field()) {
                flags.push(flag);
            }
        }
        flags
    })
}

fn meta_value(status: Status, data: Option<Vec<u8>>, flags: &[MetaFlag]) -> MetaValue {
    let mut value = MetaValue {
        status: Some(status),
        data,
        ..MetaValue::default()
    };
    for flag in flags {
        flag.apply(&mut value);
    }
    value
}

fn meta_line(code: &str, flags: &[MetaFlag]) -> Vec<u8> {
    let mut frame = code.as_bytes().to_vec();
    for flag in flags {
        flag.encode(&mut frame);
    }
    frame.extend_from_slice(b"\r\n");
    frame
}

/// A `VA` reply with its data block. A zero-length block parses as no data.
fn meta_value_reply(
    data: impl Strategy<Value = Vec<u8>>,
) -> impl Strategy<Value = (Vec<u8>, MetaResponse)> {
    (data, meta_flags()).prop_map(|(data, flags)| {
        let mut frame = meta_line(&format!("VA {}", data.len()), &flags);
        frame.extend_from_slice(&data);
        frame.extend_from_slice(b"\r\n");
        let value = meta_value(Status::Value, (!data.is_empty()).then_some(data), &flags);
        (frame, MetaResponse::Data(Some(vec![value])))
    })
}

/// A status line that parses to the bare status without flags, and to a `MetaValue` with them.
fn meta_status_reply(
    codes: Vec<(&'static str, Status)>,
) -> impl Strategy<Value = (Vec<u8>, MetaResponse)> {
    (select(codes), meta_flags()).prop_map(|((code, status), flags)| {
        let reply = if flags.is_empty() {
            MetaResponse::Status(status)
        } else {
            MetaResponse::Data(Some(vec![meta_value(status, None, &flags)]))
        };
        (meta_line(code, &flags), reply)
    })
}

/// A meta get hit or miss line, as in a pipeline.
fn meta_get_entry() -> impl Strategy<Value = (Vec<u8>, MetaResponse)> {
    // Without flags, `HD` is a hit that returns no metadata.
    let header = meta_flags().prop_map(|flags| {
        let reply = if flags.is_empty() {
            MetaResponse::Data(None)
        } else {
            MetaResponse::Data(Some(vec![meta_value(Status::Exists, None, &flags)]))
        };
        (meta_line("HD", &flags), reply)
    });
    prop_oneof![
        3 => meta_value_reply(payload()),
        1 => header,
        1 => meta_status_reply(vec![("EN", Status::NotFound)]),
    ]
}

/// Replies that every meta parser accepts: the no-op and the error lines.
fn meta_common_reply() -> impl Strategy<Value = (Vec<u8>, MetaResponse)> {
    prop_oneof![
        Just((b"MN\r\n".to_vec(), MetaResponse::Status(Status::NoOp))),
        Just((
            b"ERROR\r\n".to_vec(),
            MetaResponse::Status(Status::Error(ErrorKind::NonexistentCommand)),
        )),
        text().prop_map(|message| {
            (
                format!("SERVER_ERROR {message}\r\n").into_bytes(),
                MetaResponse::Status(Status::Error(ErrorKind::Server(message))),
            )
        }),
    ]
}

fn counter_digits() -> impl Strategy<Value = Vec<u8>> {
    any::<u64>().prop_map(|count| count.to_string().into_bytes())
}

proptest! {
    #[test]
    fn ascii_value_blocks_parse_whole_and_wait_on_every_prefix(
        values in vec(ascii_value(), 0..=3),
    ) {
        let (frame, reply) = value_block(values);
        assert_frame(parse_ascii_response, &frame, reply)?;
    }

    #[test]
    fn ascii_lines_parse_whole_and_wait_on_every_prefix((frame, reply) in ascii_line()) {
        assert_frame(parse_ascii_response, &frame, reply)?;
    }

    #[test]
    fn consecutive_ascii_replies_parse_one_at_a_time(replies in vec(ascii_reply(), 1..=4)) {
        assert_sequence(parse_ascii_response, replies)?;
    }

    #[test]
    fn counter_replies_beyond_u64_are_protocol_errors(
        count in prop_oneof![
            (u64::MAX as u128 + 1)..=99_999_999_999_999_999_999u128,
            (u64::MAX as u128 + 1)..=u128::MAX,
        ],
    ) {
        let result = parse_ascii_response(format!("{count}\r\n").as_bytes());
        prop_assert!(matches!(result, Err(ErrorKind::Protocol(_))), "{:?}", result);
    }

    #[test]
    fn metadump_lines_parse_whole_and_wait_on_every_prefix((frame, reply) in metadump_reply()) {
        assert_frame(parse_ascii_metadump_response, &frame, reply)?;
    }

    #[test]
    fn consecutive_metadump_lines_parse_one_at_a_time(replies in vec(metadump_reply(), 1..=4)) {
        assert_sequence(parse_ascii_metadump_response, replies)?;
    }

    #[test]
    fn stats_lines_parse_whole_and_wait_on_every_prefix((frame, reply) in stats_reply()) {
        assert_frame(parse_ascii_stats_response, &frame, reply)?;
    }

    #[test]
    fn meta_get_replies_parse_whole_and_wait_on_every_prefix(
        (frame, reply) in prop_oneof![3 => meta_get_entry(), 1 => meta_common_reply()],
    ) {
        assert_frame(parse_meta_get_response, &frame, reply)?;
    }

    #[test]
    fn meta_get_pipelines_parse_one_reply_at_a_time(mut replies in vec(meta_get_entry(), 0..=4)) {
        replies.push((b"MN\r\n".to_vec(), MetaResponse::Status(Status::NoOp)));
        assert_sequence(parse_meta_get_response, replies)?;
    }

    #[test]
    fn meta_set_replies_parse_whole_and_wait_on_every_prefix(
        (frame, reply) in prop_oneof![
            3 => meta_status_reply(vec![
                ("HD", Status::Stored),
                ("NS", Status::NotStored),
                ("EX", Status::Exists),
                ("NF", Status::NotFound),
            ]),
            1 => meta_common_reply(),
        ],
    ) {
        assert_frame(parse_meta_set_response, &frame, reply)?;
    }

    #[test]
    fn meta_delete_replies_parse_whole_and_wait_on_every_prefix(
        (frame, reply) in prop_oneof![
            3 => meta_status_reply(vec![
                ("HD", Status::Deleted),
                ("NF", Status::NotFound),
                ("EX", Status::Exists),
            ]),
            1 => meta_common_reply(),
        ],
    ) {
        assert_frame(parse_meta_delete_response, &frame, reply)?;
    }

    #[test]
    fn meta_arithmetic_replies_parse_whole_and_wait_on_every_prefix(
        (frame, reply) in prop_oneof![
            2 => meta_value_reply(counter_digits()),
            2 => meta_status_reply(vec![
                ("HD", Status::Stored),
                ("NF", Status::NotFound),
                ("NS", Status::NotStored),
                ("EX", Status::Exists),
            ]),
            1 => meta_common_reply(),
        ],
    ) {
        assert_frame(parse_meta_arithmetic_response, &frame, reply)?;
    }

    #[test]
    fn numeric_meta_flags_parse_exactly_or_fail_the_reply(
        letter in select(&b"cflst"[..]),
        token in vec(
            prop_oneof![
                3 => b'0'..=b'9',
                1 => select(&b"-+"[..]),
                1 => any::<u8>().prop_filter("token byte", |byte| *byte != b' ' && *byte != b'\r'),
            ],
            1..=24,
        ),
    ) {
        let mut frame = b"VA 1 ".to_vec();
        frame.push(letter);
        frame.extend_from_slice(&token);
        frame.extend_from_slice(b"\r\nx\r\n");
        let mut expected = meta_value(Status::Value, Some(b"x".to_vec()), &[]);
        let text = std::str::from_utf8(&token).ok();
        let parsed = match letter {
            b'c' => text.and_then(|t| t.parse().ok()).map(|v| expected.cas = Some(v)),
            b'f' => text.and_then(|t| t.parse().ok()).map(|v| expected.flags = Some(v)),
            b'l' => text.and_then(|t| t.parse().ok()).map(|v| expected.last_accessed = Some(v)),
            b's' => text.and_then(|t| t.parse().ok()).map(|v| expected.size = Some(v)),
            _ => text.and_then(|t| t.parse().ok()).map(|v| expected.ttl_remaining = Some(v)),
        };
        let result = parse_meta_get_response(&frame);
        if parsed.is_some() {
            let reply = MetaResponse::Data(Some(vec![expected]));
            prop_assert_eq!(result, Ok(Some((frame.len(), reply))));
        } else {
            prop_assert!(matches!(result, Err(ErrorKind::Protocol(_))), "{:?}", result);
        }
    }

    #[test]
    fn ascii_parsers_return_incomplete_an_error_or_one_bounded_reply(bytes in wire_bytes()) {
        assert_bounded(parse_ascii_response, &bytes)?;
        assert_bounded(parse_ascii_metadump_response, &bytes)?;
        assert_bounded(parse_ascii_stats_response, &bytes)?;
    }

    #[test]
    fn meta_parsers_return_incomplete_an_error_or_one_bounded_reply(bytes in wire_bytes()) {
        let parsers: [Parse<MetaResponse>; 4] = [
            parse_meta_get_response,
            parse_meta_set_response,
            parse_meta_delete_response,
            parse_meta_arithmetic_response,
        ];
        for parse in parsers {
            assert_bounded(parse, &bytes)?;
        }
    }
}
