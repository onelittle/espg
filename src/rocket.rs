use std::{convert::Infallible, marker::PhantomData};

use crate::Id;
use rocket::request::FromParam;

impl<T> FromParam<'_> for Id<T> {
    type Error = Infallible;

    fn from_param(param: &str) -> Result<Self, Self::Error> {
        Ok(Id(param.to_string(), PhantomData))
    }
}
