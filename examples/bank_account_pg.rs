use espg::{
    Aggregate, EventStore, Id, PostgresEventStore, PostgresEventStream, StreamItem,
    StreamingEventStore,
};
use futures::Stream;
use serde::{Deserialize, Serialize};
use tokio_postgres::NoTls;
use tokio_stream::StreamExt;

#[derive(Default, Serialize, Deserialize)]
struct AccountState {
    active: bool,
    balance: i64,
}

#[derive(Clone, Serialize, Deserialize)]
enum Event {
    AccountOpened,
    MoneyDeposited(i64),
    MoneyWithdrawn(i64),
    AccountClosed,
}

impl Aggregate for AccountState {
    const NAME: &'static str = "AccountState";
    type Event = Event;

    fn reduce(mut self, event: &Self::Event) -> Self {
        match event {
            Event::AccountOpened => {
                self.active = true;
                self.balance = 0;
            }
            Event::MoneyDeposited(amount) => {
                if self.active {
                    self.balance += amount;
                }
            }
            Event::MoneyWithdrawn(amount) => {
                if self.active && self.balance >= *amount {
                    self.balance -= amount;
                }
            }
            Event::AccountClosed => {
                self.active = false;
            }
        }
        self
    }
}

fn update_stats_display(active_accounts: usize, total_balance: i64) {
    println!(
        "Active accounts: {} | Total balance: {}",
        active_accounts, total_balance
    );
}

async fn open_account(event_store: &impl EventStore) -> Result<Id<AccountState>, espg::Error> {
    let id = uuid::Uuid::new_v4().to_string();
    let id = AccountState::id(&id);
    event_store.commit(&id, 1, Event::AccountOpened).await?;
    Ok(id)
}

async fn deposit_money(
    event_store: &impl EventStore,
    id: &Id<AccountState>,
    amount: i64,
) -> Result<(), espg::Error> {
    event_store.append(id, Event::MoneyDeposited(amount)).await
}

async fn withdraw_money(
    event_store: &impl EventStore,
    id: &Id<AccountState>,
    amount: i64,
) -> Result<(), espg::Error> {
    event_store
        .retry_on_version_conflict(|| async {
            #[allow(clippy::unwrap_used)]
            let commit = event_store.get_commit(id).await.unwrap();
            event_store
                .commit(id, commit.version + 1, Event::MoneyWithdrawn(amount))
                .await
        })
        .await
}

async fn close_account(
    event_store: &impl EventStore,
    id: &Id<AccountState>,
) -> Result<(), espg::Error> {
    event_store.append(id, Event::AccountClosed).await
}

#[allow(clippy::expect_used)]
async fn init_event_stream() -> impl Stream<Item = StreamItem<Event>> {
    let connection_string = "postgres://theodorton@localhost/espg_examples".to_string();
    let config: tokio_postgres::Config = connection_string
        .parse()
        .expect("Failed to parse Postgres config");

    let es = PostgresEventStream::new(config)
        .await
        .expect("Failed to create Postgres event stream");
    es.stream::<AccountState>()
        .await
        .expect("Failed to create event stream")
}

#[tokio::main]
async fn main() -> espg::Result<()> {
    let connection_string = "postgres://theodorton@localhost/espg_examples".to_string();
    let (client, connection) = tokio_postgres::connect(&connection_string, NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });

    let mut active_accounts = 0;
    let mut total_balance = 0;
    PostgresEventStore::initialize(&client).await?;
    PostgresEventStore::clear(&client).await?;

    let stream = init_event_stream().await;

    let event_store = PostgresEventStore::new(&client);
    let id = open_account(&event_store).await?;
    deposit_money(&event_store, &id, 100).await?;
    withdraw_money(&event_store, &id, 50).await?;
    let id2 = open_account(&event_store).await?;
    deposit_money(&event_store, &id2, 200).await?;
    close_account(&event_store, &id2).await?;

    let thread_b = {
        let id = id.clone();
        tokio::spawn(async move {
            let event_store = PostgresEventStore::new(&client);
            deposit_money(&event_store, &id, 100).await?;
            withdraw_money(&event_store, &id, 50).await?;

            espg::Result::Ok(())
        })
    };

    match tokio::try_join!(thread_b) {
        Ok(_) => println!("All commands executed successfully."),
        Err(e) => eprintln!("Error executing commands: {}", e),
    }

    let mut n = 0;
    let mut stream = stream.take(8);
    while let Some(commit) = stream.next().await {
        let Ok(commit) = commit else {
            eprintln!("Error processing event stream");
            break;
        };

        n += 1;
        eprintln!("Received event number {}", n);
        let event = commit.inner;
        match event {
            Event::AccountOpened => {
                active_accounts += 1;
                total_balance += 0; // New account starts with zero balance
            }
            Event::MoneyDeposited(amount) => {
                total_balance += amount;
            }
            Event::MoneyWithdrawn(amount) => {
                total_balance -= amount;
            }
            Event::AccountClosed => {
                active_accounts -= 1;
            }
        }

        update_stats_display(active_accounts, total_balance);
    }
    eprintln!("Event stream ended.");

    Ok(())
}
