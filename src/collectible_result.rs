use crate::error::Error;

pub(crate) struct CollectibleResult<T> {
    pub inner: Result<Vec<T>, Error>,
}

impl<T> Default for CollectibleResult<T> {
    fn default() -> Self {
        CollectibleResult {
            inner: Ok(Vec::<T>::new()),
        }
    }
}

impl<T> Extend<Result<Vec<T>, Error>> for CollectibleResult<T> {
    fn extend<I>(&mut self, iter: I)
    where
        I: IntoIterator<Item = Result<Vec<T>, Error>>,
    {
        if self.inner.is_err() {
            return;
        }

        let result = self.inner.as_mut().unwrap();
        for item in iter {
            match item {
                Ok(mut vec) => result.append(&mut vec),
                Err(e) => {
                    self.inner = Err(e);
                    return;
                }
            }
        }
    }
}

impl<T> Extend<Result<T, Error>> for CollectibleResult<T> {
    fn extend<I>(&mut self, iter: I)
    where
        I: IntoIterator<Item = Result<T, Error>>,
    {
        if self.inner.is_err() {
            return;
        }

        let result = self.inner.as_mut().unwrap();
        for item in iter {
            match item {
                Ok(val) => result.push(val),
                Err(e) => {
                    self.inner = Err(e);
                    return;
                }
            }
        }
    }
}
