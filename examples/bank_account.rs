use espg::{Aggregate, EventStore, InMemoryEventStore};
use tokio_stream::StreamExt;

#[derive(Default)]
struct AccountState {
    active: bool,
    balance: i64,
}

#[derive(Clone)]
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

struct Commands<'a> {
    event_store: &'a mut InMemoryEventStore<AccountState>,
}

impl<'a> Commands<'a> {
    fn new(event_store: &'a mut InMemoryEventStore<AccountState>) -> Self {
        Commands { event_store }
    }

    async fn open_account(&mut self, id: &str) -> Result<(), espg::Error> {
        self.event_store.append(id, Event::AccountOpened).await
    }

    async fn deposit_money(&mut self, id: &str, amount: i64) -> Result<(), espg::Error> {
        self.event_store
            .append(id, Event::MoneyDeposited(amount))
            .await
    }

    async fn withdraw_money(&mut self, id: &str, amount: i64) -> Result<(), espg::Error> {
        self.event_store
            .append(id, Event::MoneyWithdrawn(amount))
            .await
    }

    async fn close_account(&mut self, id: &str) -> Result<(), espg::Error> {
        self.event_store.append(id, Event::AccountClosed).await
    }
}

#[tokio::main]
async fn main() -> espg::Result<()> {
    let event_store: InMemoryEventStore<AccountState> = InMemoryEventStore::default();
    let mut active_accounts = 0;
    let mut total_balance = 0;

    let stream_handler = {
        let event_store = event_store.clone();
        tokio::spawn(async move {
            let mut stream = event_store.stream().await;
            while let Some(Ok(commit)) = stream.next().await {
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
        })
    };

    let thread_a = {
        let mut event_store = event_store.clone();
        tokio::spawn(async move {
            let mut commands = Commands::new(&mut event_store);
            commands.open_account("account1").await?;
            commands.deposit_money("account1", 100).await?;
            commands.withdraw_money("account1", 50).await?;
            commands.open_account("account2").await?;
            commands.deposit_money("account2", 200).await?;
            commands.close_account("account2").await?;

            espg::Result::Ok(())
        })
    };

    let thread_b = {
        let mut event_store = event_store.clone();
        tokio::spawn(async move {
            let mut commands = Commands::new(&mut event_store);
            commands.deposit_money("account1", 100).await?;
            commands.withdraw_money("account1", 50).await?;

            espg::Result::Ok(())
        })
    };

    match tokio::try_join!(thread_a, thread_b) {
        Ok(_) => println!("All commands executed successfully."),
        Err(e) => eprintln!("Error executing commands: {}", e),
    }

    stream_handler.abort();

    Ok(())
}
