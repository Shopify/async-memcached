use std::str;

mod private {
    pub trait AsMemcachedValue {
        fn as_bytes(&self) -> std::borrow::Cow<'_, [u8]>;
    }
}

/// A trait for serializing multiple types of values in to appropriate memcached input values for the set and add commands.
pub trait AsMemcachedValue: private::AsMemcachedValue {}

impl<T: private::AsMemcachedValue> AsMemcachedValue for T {}

impl private::AsMemcachedValue for &[u8] {
    fn as_bytes(&self) -> std::borrow::Cow<'_, [u8]> {
        std::borrow::Cow::Borrowed(self)
    }
}

impl private::AsMemcachedValue for bytes::Bytes {
    fn as_bytes(&self) -> std::borrow::Cow<'_, [u8]> {
        std::borrow::Cow::Borrowed(self.as_ref())
    }
}

impl private::AsMemcachedValue for &str {
    fn as_bytes(&self) -> std::borrow::Cow<'_, [u8]> {
        std::borrow::Cow::Borrowed(str::as_bytes(self))
    }
}

impl private::AsMemcachedValue for &String {
    fn as_bytes(&self) -> std::borrow::Cow<'_, [u8]> {
        std::borrow::Cow::Borrowed(str::as_bytes(self))
    }
}

macro_rules! impl_to_memcached_value_for_uint {
    ($ty:ident) => {
        impl private::AsMemcachedValue for $ty {
            fn as_bytes(&self) -> std::borrow::Cow<'_, [u8]> {
                std::borrow::Cow::Owned(self.to_string().into_bytes())
            }
        }
    };
}

impl_to_memcached_value_for_uint!(u8);
impl_to_memcached_value_for_uint!(u16);
impl_to_memcached_value_for_uint!(u32);
impl_to_memcached_value_for_uint!(u64);
impl_to_memcached_value_for_uint!(usize);
