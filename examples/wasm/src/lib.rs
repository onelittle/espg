use serde::{Deserialize, Serialize};
use wasm_bindgen::prelude::*;

/// Aggregate trait (simplified for wasm - no Send/Sync bounds)
pub trait Aggregate: Default + Serialize + for<'de> Deserialize<'de> {
    type Event: Serialize + for<'de> Deserialize<'de>;
    fn reduce(self, event: &Self::Event) -> Self;
}

// Example: Bank Account
#[derive(Default, Serialize, Deserialize)]
pub struct AccountState {
    pub balance: i32,
}

#[derive(Serialize, Deserialize)]
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
///
/// # Arguments
/// * `state` - JSON object representing the current state (e.g., `{ "balance": 100 }`)
/// * `event` - JSON object representing the event (e.g., `{ "Deposit": 50 }`)
///
/// # Returns
/// JSON object representing the new state
#[wasm_bindgen]
pub fn apply_event(state: JsValue, event: JsValue) -> Result<JsValue, JsValue> {
    let state: AccountState =
        serde_wasm_bindgen::from_value(state).map_err(|e| JsValue::from_str(&e.to_string()))?;
    let event: AccountEvent =
        serde_wasm_bindgen::from_value(event).map_err(|e| JsValue::from_str(&e.to_string()))?;

    let new_state = state.reduce(&event);

    serde_wasm_bindgen::to_value(&new_state).map_err(|e| JsValue::from_str(&e.to_string()))
}
