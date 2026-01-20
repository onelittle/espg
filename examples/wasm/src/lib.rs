use serde::{Deserialize, Serialize};
use tsify_next::Tsify;
use wasm_bindgen::prelude::*;

/// Aggregate trait (simplified for wasm - no Send/Sync bounds)
pub trait Aggregate: Default + Serialize + for<'de> Deserialize<'de> {
    type Event: Serialize + for<'de> Deserialize<'de>;
    fn reduce(self, event: &Self::Event) -> Self;
}

// Example: Bank Account
#[derive(Default, Serialize, Deserialize, Tsify)]
#[tsify(into_wasm_abi, from_wasm_abi)]
pub struct AccountState {
    pub balance: i32,
}

#[derive(Serialize, Deserialize, Tsify)]
#[tsify(into_wasm_abi, from_wasm_abi)]
pub enum AccountEvent {
    Deposit(i32),
    Withdraw(i32),
}

impl Aggregate for AccountState {
    type Event = AccountEvent;

    fn reduce(mut self, event: &Self::Event) -> Self {
        match event {
            AccountEvent::Deposit(amount) => self.balance += amount,
            AccountEvent::Withdraw(amount) => self.balance -= amount,
        }
        self
    }
}

/// Apply an event to a state and return the new state.
#[wasm_bindgen]
pub fn apply_event(state: AccountState, event: AccountEvent) -> AccountState {
    state.reduce(&event)
}
