use std::marker::PhantomData;

use crate::Id;
use rocket::request::FromParam;

impl<T> FromParam<'_> for Id<T> {
    type Error = String;

    fn from_param(param: &str) -> Result<Self, Self::Error> {
        Ok(Id(param.to_string(), PhantomData))
    }
}
