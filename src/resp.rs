use std::fmt::{self, Write};

use anyhow::Result;

#[derive(Debug)]
pub enum Data<'a> {
    SimpleString(&'a str),
    SimpleError(&'a str),
    Integer(i64),
    BulkString(Option<&'a str>),
    Array(Vec<Data<'a>>),
}

impl<'a> fmt::Display for Data<'_> {
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
        let (fb, data) = data.split_first().expect("empty data");
        match fb {
            b'+' => {
                let idx = data.iter().position(|&c| c == b'\r').expect("no crlf");
                let s = std::str::from_utf8(&data[..idx])?;
                let remaining = &data[idx + 2..];
                Ok((Data::SimpleString(s), remaining))
            }
            b'-' => {
                let idx = data.iter().position(|&c| c == b'\r').expect("no crlf");
                let s = std::str::from_utf8(&data[..idx])?;
                let remaining = &data[idx + 2..];
                Ok((Data::SimpleError(s), remaining))
            }
            b':' => {
                let idx = data.iter().position(|&c| c == b'\r').expect("no crlf");
                let s = std::str::from_utf8(&data[..idx])?.parse()?;
                let remaining = &data[idx + 2..];
                Ok((Data::Integer(s), remaining))
            }
            b'$' => {
                let idx = data.iter().position(|&c| c == b'\r').expect("no crlf");
                let len: usize = std::str::from_utf8(&data[..idx])?.parse()?;
                let s = std::str::from_utf8(&data[idx + 2..idx + 2 + len])?;
                let remaining = &data[idx + 2 + len + 2..];
                Ok((Data::BulkString(Some(s)), remaining))
            }
            b'*' => {
                let idx = data.iter().position(|&c| c == b'\r').expect("no crlf");
                let len: usize = std::str::from_utf8(&data[..idx])?.parse()?;

                let mut arr = Vec::with_capacity(len);

                let mut remaining = &data[idx + 2..];
                for _ in 0..len {
                    let (d, rem): (Data<'_>, &[u8]) = Data::decode(remaining)?;
                    arr.push(d);
                    remaining = rem;
                }
                let arr = Data::Array(arr);
                Ok((arr, remaining))
            }
            _ => {
                unimplemented!();
            }
        }
    }
}
