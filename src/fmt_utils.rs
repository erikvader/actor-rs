use derive_more::*;

#[repr(transparent)]
#[derive(Display, From, Clone)]
pub struct DisplayMe<T>(pub T);

impl<T> std::fmt::Debug for DisplayMe<T>
where
    T: std::fmt::Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
