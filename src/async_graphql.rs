use indexmap::IndexMap;

use crate::{Commit, Id};

#[cfg(feature = "async-graphql")]
#[async_graphql::Scalar]
impl<T: Send + Sync> async_graphql::ScalarType for Id<T> {
    fn parse(value: async_graphql::Value) -> async_graphql::InputValueResult<Self> {
        if let async_graphql::Value::String(s) = &value {
            use std::marker::PhantomData;

            Ok(Id(s.clone(), PhantomData))
        } else {
            Err(async_graphql::InputValueError::expected_type(value))
        }
    }

    fn to_value(&self) -> async_graphql::Value {
        async_graphql::Value::String(self.0.clone())
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

            fields.insert(
                "id".to_string(),
                async_graphql::registry::MetaField {
                    name: "id".to_string(),
                    description: None,
                    args: Default::default(),
                    ty: "ID!".to_string(),
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
                },
            );

            fields.insert(
                "version".to_string(),
                async_graphql::registry::MetaField {
                    name: "version".to_string(),
                    description: None,
                    args: Default::default(),
                    ty: "Int!".to_string(),
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
                },
            );

            fields.insert(
                "inner".to_string(),
                async_graphql::registry::MetaField {
                    name: "inner".to_string(),
                    description: None,
                    args: Default::default(),
                    ty: format!("{}!", T::type_name()),
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

    #[tokio::test]
    async fn test_commit_type_name() {
        use async_graphql::OutputType;

        use crate::tests::State;

        assert_eq!(State::type_name(), "State");
        assert_eq!(Commit::<State>::type_name(), "StateCommit");
    }
}
