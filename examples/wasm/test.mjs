import { apply_event } from './pkg/espg_wasm_example.js';

// Test initial state with deposit
const state1 = { balance: 100 };
const result1 = apply_event(state1, { Deposit: 50 });
console.assert(result1.balance === 150, `Expected 150, got ${result1.balance}`);

// Test withdrawal
const result2 = apply_event(result1, { Withdraw: 30 });
console.assert(result2.balance === 120, `Expected 120, got ${result2.balance}`);

// Test from zero balance
const state3 = { balance: 0 };
const result3 = apply_event(state3, { Deposit: 1000 });
console.assert(result3.balance === 1000, `Expected 1000, got ${result3.balance}`);

// Test negative balance (withdrawal beyond balance)
const result4 = apply_event(state3, { Withdraw: 50 });
console.assert(result4.balance === -50, `Expected -50, got ${result4.balance}`);

console.log('All tests passed!');
