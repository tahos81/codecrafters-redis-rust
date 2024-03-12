use anyhow::{bail, Context, Result};
use std::{
    fmt::{self, Write},
    str,
};

use crate::command::Command;

#[derive(Debug)]
pub enum Data<'a> {
    SimpleString(&'a str),
    SimpleError(&'a str),
    Integer(i64),
    BulkString(Option<&'a str>),
    Array(Vec<Data<'a>>),
}

impl fmt::Display for Data<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Data::SimpleString(s) => write!(f, "+{s}\r\n"),
            Data::SimpleError(s) => write!(f, "-{s}\r\n"),
            Data::Integer(i) => write!(f, ":{i}\r\n"),
            Data::BulkString(s) => match s {
                Some(s) => {
                    write!(f, "${}\r\n{}\r\n", s.len(), s)
                }
                None => write!(f, "$-1\r\n"),
            },
            Data::Array(arr) => {
                write!(f, "*{}\r\n", arr.len())?;
                for a in arr {
                    write!(f, "{a}")?;
                }
                Ok(())
            }
        }
    }
}

impl<'a> From<Command<'a>> for Data<'a> {
    fn from(cmd: Command<'a>) -> Self {
        match cmd {
            Command::Ping { message } => {
                if let Some(message) = message {
                    Data::Array(vec![Data::BulkString(Some(message))])
                } else {
                    Data::Array(vec![Data::BulkString(Some("PING"))])
                }
            }
            _ => unimplemented!(),
        }
    }
}

impl<'a> Data<'a> {
    pub async fn write_to<WR: tokio::io::AsyncWriteExt + std::marker::Unpin>(
        &self,
        w: &mut WR,
    ) -> Result<()> {
        match self {
            Data::SimpleString(s) => {
                w.write_all(b"+").await?;
                w.write_all(s.as_bytes()).await?;
                w.write_all(b"\r\n").await?;
                w.flush().await?;
                Ok(())
            }
            Data::SimpleError(s) => {
                w.write_all(b"-").await?;
                w.write_all(s.as_bytes()).await?;
                w.write_all(b"\r\n").await?;
                w.flush().await?;
                Ok(())
            }
            Data::Integer(i) => {
                w.write_all(b":").await?;
                w.write_all(i.to_string().as_bytes()).await?;
                w.write_all(b"\r\n").await?;
                w.flush().await?;
                Ok(())
            }
            Data::BulkString(s) => match s {
                Some(s) => {
                    w.write_all(b"$").await?;
                    w.write_all(s.len().to_string().as_bytes()).await?;
                    w.write_all(b"\r\n").await?;
                    w.write_all(s.as_bytes()).await?;
                    w.write_all(b"\r\n").await?;
                    w.flush().await?;
                    Ok(())
                }
                None => {
                    w.write_all(b"$-1\r\n").await?;
                    w.flush().await?;
                    Ok(())
                }
            },
            Data::Array(arr) => {
                w.write_all(b"*").await?;
                w.write_all(arr.len().to_string().as_bytes()).await?;
                w.write_all(b"\r\n").await?;
                let mut s = String::new();
                for a in arr {
                    write!(s, "{a}")?;
                }
                w.write_all(s.as_bytes()).await?;
                Ok(())
            }
        }
    }

    pub fn decode(data: &'a [u8]) -> Result<(Data<'a>, &'a [u8])> {
        let (fb, data) = data.split_first().with_context(|| "RESP empty data")?;
        match fb {
            b'+' => {
                let (data, remaining) = parse_line(data)?;
                let s = parse_utf8(data)?;
                Ok((Data::SimpleString(s), remaining))
            }
            b'-' => {
                let (data, remaining) = parse_line(data)?;
                let s = parse_utf8(data)?;
                Ok((Data::SimpleError(s), remaining))
            }
            b':' => {
                let (data, remaining) = parse_line(data)?;
                let i = parse_integer(data)?;
                Ok((Data::Integer(i), remaining))
            }
            b'$' => {
                let (data, remaining) = parse_line(data)?;
                let len = parse_integer(data)?;
                let (s, remaining) = if len == -1 {
                    (None, remaining)
                } else {
                    let (s, remaining) = remaining.split_at(len as usize);
                    let s = parse_utf8(s)?;
                    let remaining = skip_crlf(remaining)?;
                    (Some(s), remaining)
                };
                Ok((Data::BulkString(s), remaining))
            }
            b'*' => {
                let (data, mut remaining) = parse_line(data)?;
                let len = parse_integer(data)?;

                if len == -1 {
                    return Ok((Data::Array(Vec::new()), remaining));
                }

                let len = len as usize;

                let mut arr = Vec::with_capacity(len);

                for _ in 0..len {
                    let (d, rem): (Data<'_>, &[u8]) = Data::decode(remaining)?;
                    arr.push(d);
                    remaining = rem;
                }
                let arr = Data::Array(arr);
                Ok((arr, remaining))
            }
            _ => Err(anyhow::anyhow!("UNIMPLEMENTED unknown data type")),
        }
    }
}

fn parse_line(data: &[u8]) -> Result<(&[u8], &[u8])> {
    let idx = data
        .windows(2)
        .position(|window| window == b"\r\n")
        .with_context(|| "RESP no crlf")?;
    let (data, remaining) = data.split_at(idx);
    let remaining = skip_crlf(remaining)?;
    Ok((data, remaining))
}

fn parse_utf8(data: &[u8]) -> Result<&str> {
    str::from_utf8(data).with_context(|| "RESP invalid utf8")
}

fn parse_integer(data: &[u8]) -> Result<i64> {
    parse_utf8(data).and_then(|s| s.parse().with_context(|| "RESP invalid integer"))
}

fn skip_crlf(data: &[u8]) -> Result<&[u8]> {
    if data.len() < 2 || &data[..2] != b"\r\n" {
        bail!("RESP no crlf");
    }

    Ok(&data[2..])
}
