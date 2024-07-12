package main

import (
	"testing"
)

func TestTransactionBalances(t *testing.T) {
	accounts := []Account{
		{ID: 1, Balance: 500000},
		{ID: 2, Balance: 500000},
		{ID: 3, Balance: 0},
		{ID: 4, Balance: 0},
	}

	generateTransactions(accounts, 5000000)

	expectedBalance := 250000.0
	for _, account := range accounts {
		if account.Balance != expectedBalance {
			t.Errorf("Account %d balance incorrect: got $%.2f, want $%.2f", account.ID, account.Balance, expectedBalance)
		}
	}
}
