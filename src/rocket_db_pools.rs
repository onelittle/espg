use crate::{PostgresEventStore, event_stores::postgres::GenericClientStore};
use rocket::request::FromRequest;
use tokio_postgres::GenericClient;

impl<D: rocket_db_pools::Database<Pool = rocket_db_pools::deadpool_postgres::Pool>>
    GenericClientStore for PostgresEventStore<rocket_db_pools::Connection<D>>
{
    fn get_client(&self) -> &impl GenericClient {
        let dereffed: &tokio_postgres::Client = &self.client;
        dereffed
    }

    fn count_read(&self) {
        // No-op for Rocket connection
    }

    fn count_write(&self) {
        // No-op for Rocket connection
    }
}

#[rocket::async_trait]
impl<'r, D: rocket_db_pools::Database<Pool = rocket_db_pools::deadpool_postgres::Pool>>
    FromRequest<'r> for PostgresEventStore<rocket_db_pools::Connection<D>>
{
    type Error = Option<rocket_db_pools::Error<rocket_db_pools::deadpool_postgres::PoolError>>;

    async fn from_request(
        request: &'r rocket::Request<'_>,
    ) -> rocket::request::Outcome<Self, Self::Error> {
        request
            .guard::<rocket_db_pools::Connection<D>>()
            .await
            .map(PostgresEventStore::new)
    }
}
