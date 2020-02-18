pub trait Apply: Sized {
    fn apply<F>(mut self, f: F) -> Self
        where F: FnOnce(&mut Self),
    {
        f(&mut self);
        self
    }
}

impl<T> Apply for T {}

/*pub trait JoinOsString {
    fn join(&self, separator: &OsStr) -> OsString;
}

impl JoinOsString for Vec<OsString> {
    fn join(&self, separator: &OsStr) -> OsString {
        let new_string_size = self
            .iter()
            .map(|string| string.len() + 1)
            .sum();

        self
            .iter()
            .fold(OsString::with_capacity(new_string_size), |mut parent, myself| {
                parent.push(separator);
                parent.push(myself);

                parent
            })
    }
}*/

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test() {
        let x = 100.apply(|myself| {
            *myself += 1;
        });

        assert_eq!(x, 101);
    }
}