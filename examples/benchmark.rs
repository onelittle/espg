use espg::{Aggregate, EventStore, Id, InMemoryEventStore};
use serde::{Deserialize, Serialize};

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

struct Commands<'a> {
    event_store: &'a mut InMemoryEventStore,
}

impl<'a> Commands<'a> {
    fn new(event_store: &'a mut InMemoryEventStore) -> Self {
        Commands { event_store }
    }

    async fn open_account(&mut self, id: &Id<AccountState>) -> Result<(), espg::Error> {
        self.event_store.append(id, Event::AccountOpened).await
    }

    async fn deposit_money(
        &mut self,
        id: &Id<AccountState>,
        amount: i64,
    ) -> Result<(), espg::Error> {
        self.event_store
            .append(id, Event::MoneyDeposited(amount))
            .await
    }

    async fn withdraw_money(
        &mut self,
        id: &Id<AccountState>,
        amount: i64,
    ) -> Result<(), espg::Error> {
        self.event_store
            .append(id, Event::MoneyWithdrawn(amount))
            .await
    }

    async fn close_account(&mut self, id: &Id<AccountState>) -> Result<(), espg::Error> {
        self.event_store.append(id, Event::AccountClosed).await
    }
}

#[tokio::main]
#[allow(clippy::unwrap_used)]
#[allow(clippy::expect_used)]
async fn main() {
    let instant = std::time::Instant::now();

    println!("Starting benchmark...");
    // Initialize the in-memory event store
    let event_store = espg::InMemoryEventStore::default();

    // Spawn 8 threads and perform 100000 operations in each
    let mut handles = vec![];
    for i in 0..8 {
        let mut event_store_clone = event_store.clone();
        let handle = tokio::spawn(async move {
            let mut commands = Commands::new(&mut event_store_clone);
            let account_id = AccountState::id(format!("account{}", i + 100_000));
            commands.open_account(&account_id).await.unwrap();
            for _ in 0..=(512_000 - 2) {
                commands.deposit_money(&account_id, 100).await.unwrap();
                commands.withdraw_money(&account_id, 50).await.unwrap();
            }
            commands.close_account(&account_id).await.unwrap();
        });
        handles.push(handle);
    }

    // Wait for all threads to complete
    for handle in handles {
        handle.await.unwrap();
    }

    let elapsed = instant.elapsed();
    let event_count = event_store.len().await;
    println!(
        "Write benchmark completed in {}ms - {} events ({}ns/event) written",
        elapsed.as_millis(),
        event_count,
        elapsed.as_nanos() / event_count as u128
    );
    let instant = std::time::Instant::now();

    let mut handles = vec![];
    for i in 0..8 {
        let event_store_clone = event_store.clone();
        let handle = tokio::spawn(async move {
            let id = AccountState::id(format!("account{}", i + 100_000));
            let state = event_store_clone
                .get_commit(&id)
                .await
                .expect("Failed to get account state");
            (state.inner.active, state.inner.balance)
        });
        handles.push(handle);
    }

    for handle in handles {
        let (active, balance) = handle.await.unwrap();
        if active {
            println!("Account is active with balance: {}", balance);
        } else {
            println!("Account is closed.");
        }
    }

    println!(
        "Read benchmark completed in {}ms",
        instant.elapsed().as_millis()
    );
}
