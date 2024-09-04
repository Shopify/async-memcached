use std::future::Future;
use std::str;

use itoa::Buffer;
use tokio::io::AsyncWriteExt;

mod private {
    pub trait Sealed {}
}

/// A trait for serializing multiple types of values in to memcached values.
pub trait ToMemcachedValue: private::Sealed {
    /// Returns the length of the value in bytes.
    fn length(&self) -> usize;
    /// Writes the value to a writer.
    fn write_to<W: AsyncWriteExt + Unpin>(
        &self,
        writer: &mut W,
    ) -> impl Future<Output = Result<(), crate::Error>>;
}

impl private::Sealed for &[u8] {}
impl private::Sealed for &str {}
impl private::Sealed for u8 {}
impl private::Sealed for u16 {}
impl private::Sealed for u32 {}
impl private::Sealed for u64 {}
impl private::Sealed for usize {}

impl ToMemcachedValue for &[u8] {
    fn length(&self) -> usize {
        <[u8]>::len(self)
    }
    async fn write_to<W: AsyncWriteExt + Unpin>(&self, writer: &mut W) -> Result<(), crate::Error> {
        writer.write_all(self).await.map_err(crate::Error::from)
    }
}

impl ToMemcachedValue for &str {
    fn length(&self) -> usize {
        <str>::len(self)
    }
    async fn write_to<W: AsyncWriteExt + Unpin>(&self, writer: &mut W) -> Result<(), crate::Error> {
        writer
            .write_all(self.as_bytes())
            .await
            .map_err(crate::Error::from)
    }
}

macro_rules! impl_to_memcached_value_for_uint {
    ($ty:ident) => {
        impl ToMemcachedValue for $ty {
            fn length(&self) -> usize {
                let mut buf = Buffer::new();

                buf.format(*self).as_bytes().len()
            }
            async fn write_to<W: AsyncWriteExt + Unpin>(
                &self,
                writer: &mut W,
            ) -> Result<(), crate::Error> {
                let mut buf = Buffer::new();

                writer
                    .write_all(buf.format(*self).as_bytes())
                    .await
                    .map_err(crate::Error::from)
            }
        }
    };
}

impl_to_memcached_value_for_uint!(u8);
impl_to_memcached_value_for_uint!(u16);
impl_to_memcached_value_for_uint!(u32);
impl_to_memcached_value_for_uint!(u64);
impl_to_memcached_value_for_uint!(usize);
