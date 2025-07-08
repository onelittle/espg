use async_graphql::{OutputType, ScalarType};
use indexmap::IndexMap;

use crate::{Aggregate, Commit, Id};

impl<X: Aggregate> ScalarType for Id<X> {
    fn parse(value: async_graphql::Value) -> async_graphql::InputValueResult<Id<X>> {
        #[allow(clippy::expect_used)]
        let val: String = async_graphql::from_value(value).expect("Failed to parse Id from value");
        ::std::result::Result::Ok(Id(val, ::std::marker::PhantomData))
    }

    fn to_value(&self) -> async_graphql::Value {
        async_graphql::Value::String(self.0.clone())
    }
}

impl<X: Aggregate + OutputType> async_graphql::InputType for Id<X> {
    type RawValueType = String;

    fn type_name() -> ::std::borrow::Cow<'static, ::std::primitive::str> {
        format!("{}ID", X::type_name()).into()
    }

    fn create_type_info(registry: &mut async_graphql::registry::Registry) -> ::std::string::String {
        registry.create_input_type::<Id<X>, _>(async_graphql::registry::MetaTypeId::Scalar, |_| {
            async_graphql::registry::MetaType::Scalar {
                name: format!("{}ID", X::type_name()),
                description: None,
                is_valid: ::std::option::Option::Some(::std::sync::Arc::new(|value| {
                    <Id<X> as async_graphql::ScalarType>::is_valid(value)
                })),
                visible: ::std::option::Option::None,
                inaccessible: false,
                tags: ::std::default::Default::default(),
                specified_by_url: None,
                directive_invocations: ::std::vec::Vec::new(),
                requires_scopes: ::std::vec::Vec::new(),
            }
        })
    }

    fn parse(
        value: ::std::option::Option<async_graphql::Value>,
    ) -> async_graphql::InputValueResult<Self> {
        <Id<X> as async_graphql::ScalarType>::parse(value.unwrap_or_default())
    }

    fn to_value(&self) -> async_graphql::Value {
        <Id<X> as async_graphql::ScalarType>::to_value(self)
    }

    fn as_raw_value(&self) -> ::std::option::Option<&Self::RawValueType> {
        ::std::option::Option::Some(&self.0)
    }
}

impl<X: Aggregate + OutputType> async_graphql::OutputType for Id<X> {
    fn type_name() -> ::std::borrow::Cow<'static, ::std::primitive::str> {
        format!("{}ID", X::type_name()).into()
    }

    fn create_type_info(registry: &mut async_graphql::registry::Registry) -> ::std::string::String {
        registry.create_output_type::<Id<X>, _>(async_graphql::registry::MetaTypeId::Scalar, |_| {
            async_graphql::registry::MetaType::Scalar {
                name: ::std::borrow::ToOwned::to_owned(&format!("{}ID", X::type_name())),
                description: None,
                is_valid: ::std::option::Option::Some(::std::sync::Arc::new(|value| {
                    <Id<X> as async_graphql::ScalarType>::is_valid(value)
                })),
                visible: ::std::option::Option::None,
                inaccessible: false,
                tags: ::std::default::Default::default(),
                specified_by_url: None,
                directive_invocations: ::std::vec::Vec::new(),
                requires_scopes: ::std::vec::Vec::new(),
            }
        })
    }

    async fn resolve(
        &self,
        _: &async_graphql::ContextSelectionSet<'_>,
        _field: &async_graphql::Positioned<async_graphql::parser::types::Field>,
    ) -> async_graphql::ServerResult<async_graphql::Value> {
        ::std::result::Result::Ok(async_graphql::ScalarType::to_value(self))
    }
}

impl<T: async_graphql::OutputType> async_graphql::OutputType for Commit<T> {
    fn type_name() -> std::borrow::Cow<'static, str> {
        format!("{}Commit", T::type_name()).into()
    }

    fn create_type_info(registry: &mut async_graphql::registry::Registry) -> String {
        let type_id: async_graphql::registry::MetaTypeId =
            async_graphql::registry::MetaTypeId::Object;
        let type_name = T::type_name().to_string();
        registry.create_output_type::<Self, _>(type_id, move |reg| {
            T::create_type_info(reg);

            let mut fields: IndexMap<String, async_graphql::registry::MetaField> =
                Default::default();

            let default_field = async_graphql::registry::MetaField {
                name: "".to_string(),
                description: None,
                args: Default::default(),
                ty: "".to_string(),
                deprecation: Default::default(),
                cache_control: Default::default(),
                external: false,
                requires: None,
                provides: None,
                shareable: false,
                inaccessible: false,
                tags: Default::default(),
                visible: None,
                compute_complexity: None,
                override_from: None,
                directive_invocations: vec![],
                requires_scopes: vec![],
            };

            fields.insert(
                "id".to_string(),
                async_graphql::registry::MetaField {
                    name: "id".to_string(),
                    ty: "ID!".to_string(),
                    ..default_field.clone()
                },
            );

            fields.insert(
                "version".to_string(),
                async_graphql::registry::MetaField {
                    name: "version".to_string(),
                    ty: "Int!".to_string(),
                    ..default_field.clone()
                },
            );

            fields.insert(
                "inner".to_string(),
                async_graphql::registry::MetaField {
                    name: "inner".to_string(),
                    ty: format!("{}!", T::type_name()),
                    ..default_field
                },
            );

            async_graphql::registry::MetaType::Object {
                name: format!("{}Commit", type_name),
                description: None,
                fields,
                cache_control: Default::default(),
                extends: false,
                shareable: false,
                resolvable: true,
                keys: None,
                visible: None,
                inaccessible: false,
                interface_object: false,
                tags: Default::default(),
                is_subscription: false,
                rust_typename: Some(std::any::type_name::<Self>()),
                directive_invocations: Default::default(),
                requires_scopes: Default::default(),
            }
        })
    }

    async fn resolve(
        &self,
        _ctx: &async_graphql::context::ContextSelectionSet<'_>,
        field: &async_graphql::Positioned<async_graphql::parser::types::Field>,
    ) -> async_graphql::ServerResult<async_graphql::Value> {
        let mut index_map = indexmap::map::IndexMap::new();
        for item in &field.node.selection_set.node.items {
            match item.node {
                async_graphql::parser::types::Selection::Field(ref field) => {
                    let name = field.node.name.node.to_string();
                    let value = match name.as_str() {
                        "id" => self.id.resolve(_ctx, field).await?,
                        "version" => self.version.resolve(_ctx, field).await?,
                        "inner" => {
                            let ctx = _ctx.with_selection_set(&field.node.selection_set);
                            self.inner.resolve(&ctx, field).await?
                        }
                        _ => continue,
                    };
                    let name: async_graphql::Name = async_graphql::Name::new(name);
                    let key = field.node.alias.as_ref().map_or(name.clone(), |alias| {
                        async_graphql::Name::new(alias.node.clone())
                    });
                    index_map.insert(key, value);
                }
                _ => continue,
            }
        }
        Ok(async_graphql::Value::Object(index_map))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::State;

    #[tokio::test]
    async fn test_commit_type_name() {
        use async_graphql::{InputType, OutputType};

        assert_eq!(State::type_name(), "State");
        assert_eq!(Commit::<State>::type_name(), "StateCommit");
        assert_eq!(<Id::<State> as OutputType>::type_name(), "StateID");
        assert_eq!(<Id::<State> as InputType>::type_name(), "StateID");
    }
}
