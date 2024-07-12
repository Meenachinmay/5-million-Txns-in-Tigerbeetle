package main

import (
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"os"
	"sort"
	"time"
)

type Account struct {
	ID      int
	Balance float64
}

type Transaction struct {
	FromID int
	ToID   int
	Amount float64
}

type TigerBeetleTransfer struct {
	ID              string `json:"id"`
	DebitAccountID  string `json:"debit_account_id"`
	CreditAccountID string `json:"credit_account_id"`
	Amount          string `json:"amount"`
	Ledger          uint64 `json:"ledger"`
	Code            uint64 `json:"code"`
}

func ToUint128(num int64) string {
	return fmt.Sprintf("%d", num)
}

func generateTargetedTransactions(accounts []Account, count int) []Transaction {
	var transactions []Transaction
	rand.Seed(time.Now().UnixNano())

	for i := 0; i < count; i++ {
		sort.Slice(accounts, func(i, j int) bool {
			return accounts[i].Balance > accounts[j].Balance
		})

		from := accounts[0]         // Account with the most money
		toIndex := rand.Intn(3) + 1 // Randomly choose among the other three accounts

		maxTransferAmount := (accounts[toIndex].Balance + from.Balance - 250000) / 2
		if maxTransferAmount > from.Balance {
			maxTransferAmount = from.Balance
		}
		if maxTransferAmount > (250000 - accounts[toIndex].Balance) {
			maxTransferAmount = 250000 - accounts[toIndex].Balance
		}

		amount := rand.Float64() * maxTransferAmount
		if amount < 1 {
			amount = 1 // Minimum transaction amount
		}

		transactions = append(transactions, Transaction{
			FromID: from.ID,
			ToID:   accounts[toIndex].ID,
			Amount: amount,
		})

		accounts[0].Balance -= amount
		accounts[toIndex].Balance += amount
	}

	return transactions
}

func adjustBalances(accounts []Account) {
	var total float64 = 0

	// Round balances and calculate total
	for i := range accounts {
		accounts[i].Balance = math.Round(accounts[i].Balance)
		total += accounts[i].Balance
	}

	// Calculate discrepancy
	discrepancy := 1000000 - total

	// Adjust balances to remove discrepancy
	for discrepancy != 0 {
		for i := range accounts {
			if discrepancy > 0 && accounts[i].Balance < 250000 {
				accounts[i].Balance += 1
				discrepancy -= 1
			} else if discrepancy < 0 && accounts[i].Balance > 250000 {
				accounts[i].Balance -= 1
				discrepancy += 1
			}

			if discrepancy == 0 {
				break
			}
		}
	}
}

func validateBalances(accounts []Account) bool {
	for _, account := range accounts {
		if account.Balance != 250000 {
			return false
		}
	}
	return true
}

func main() {
	accounts := []Account{
		{ID: 1, Balance: 500000},
		{ID: 2, Balance: 500000},
		{ID: 3, Balance: 0},
		{ID: 4, Balance: 0},
	}

	transactions := generateTargetedTransactions(accounts, 5000000)
	adjustBalances(accounts)

	if validateBalances(accounts) {
		// Convert transactions to TigerBeetle format and serialize to JSON
		tigerBeetleTransfers := make([]TigerBeetleTransfer, len(transactions))
		for i, t := range transactions {
			tigerBeetleTransfers[i] = TigerBeetleTransfer{
				ID:              ToUint128(int64(i + 1)),
				DebitAccountID:  ToUint128(int64(t.FromID)),
				CreditAccountID: ToUint128(int64(t.ToID)),
				Amount:          ToUint128(int64(t.Amount)),
				Ledger:          1,
				Code:            1,
			}
		}

		file, err := os.Create("transfers.json")
		if err != nil {
			fmt.Printf("Error creating JSON file: %s\n", err)
			return
		}
		defer file.Close()
		encoder := json.NewEncoder(file)
		if err := encoder.Encode(tigerBeetleTransfers); err != nil {
			fmt.Printf("Error encoding JSON: %s\n", err)
			return
		}

		fmt.Println("Transaction JSON file created successfully.")
	} else {
		fmt.Println("Validation failed: balances do not match expected values.")
	}
}
