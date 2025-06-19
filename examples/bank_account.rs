use espg::{Aggregate, Commands as _, EventStore, InMemoryEventStore, StreamingEventStore};
use serde::{Deserialize, Serialize};
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

#[derive(Clone)]
struct Commands<'a> {
    event_store: &'a InMemoryEventStore<AccountState>,
}

impl<'a> espg::Commands<'a, InMemoryEventStore<AccountState>, AccountState> for Commands<'a> {
    fn new(event_store: &'a InMemoryEventStore<AccountState>) -> Self {
        Self { event_store }
    }

    fn event_store(&'a self) -> &'a InMemoryEventStore<AccountState> {
        self.event_store
    }
}

impl<'a> Commands<'a> {
    async fn open_account(&'a self) -> Result<String, espg::Error> {
        let id = uuid::Uuid::new_v4().to_string();
        self.commit(&id, 1, Event::AccountOpened).await?;
        Ok(id)
    }

    async fn deposit_money(&'a self, id: &str, amount: i64) -> Result<(), espg::Error> {
        self.append(id, Event::MoneyDeposited(amount)).await
    }

    async fn withdraw_money(&'a self, id: &str, amount: i64) -> Result<(), espg::Error> {
        self.retry_on_version_conflict(|| async {
            #[allow(clippy::unwrap_used)]
            let commit = self.event_store.get_commit(id).await.unwrap();
            self.event_store
                .commit(id, commit.version + 1, Event::MoneyWithdrawn(amount))
                .await
        })
        .await
    }

    async fn close_account(&'a self, id: &str) -> Result<(), espg::Error> {
        self.append(id, Event::AccountClosed).await
    }
}

#[tokio::main]
async fn main() -> espg::Result<()> {
    let mut active_accounts = 0;
    let mut total_balance = 0;

    let event_store: InMemoryEventStore<AccountState> = InMemoryEventStore::default();
    let stream = event_store.clone().stream().await?;

    let commands = Commands::new(&event_store);
    let id = commands.open_account().await?;
    commands.deposit_money(&id, 100).await?;
    commands.withdraw_money(&id, 50).await?;
    let id2 = commands.open_account().await?;
    commands.deposit_money(&id2, 200).await?;
    commands.close_account(&id2).await?;

    let thread_b = {
        let event_store = event_store.clone();
        tokio::spawn(async move {
            let commands = Commands::new(&event_store);
            let id = commands.open_account().await?;
            commands.deposit_money(&id, 100).await?;
            commands.withdraw_money(&id, 50).await?;

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
    eprintln!("Event stream ended. {} events processed.", n);

    Ok(())
}
