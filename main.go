package main

import (
	"bufio"
	"fmt"
	tigerbeetle_go "github.com/tigerbeetle/tigerbeetle-go"
	. "github.com/tigerbeetle/tigerbeetle-go/pkg/types"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
	"time"
)

// Custom parsing logic to handle the format:
// "id=1, debit_account_id=1, credit_account_id=1, amount=1, ledger=1, code=1"
// parseLine processes a single line from a custom formatted text.
func parseLine(line string) (Transfer, error) {
	transfer := Transfer{}
	fields := strings.Split(line, ", ")
	for _, field := range fields {
		parts := strings.Split(field, "=")
		if len(parts) != 2 {
			return Transfer{}, fmt.Errorf("invalid field format")
		}
		key, value := parts[0], parts[1]

		// Parse float and round to nearest integer for 'amount'
		if key == "amount" {
			fval, err := strconv.ParseFloat(value, 64)
			if err != nil {
				return Transfer{}, fmt.Errorf("error parsing float from string: %v", err)
			}
			ival := uint64(math.Round(fval)) // Convert float to nearest integer
			transfer.Amount = ToUint128(ival)
			continue
		}

		// Handle other uint values
		val, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return Transfer{}, fmt.Errorf("error parsing uint from string: %v", err)
		}
		switch key {
		case "id":
			transfer.ID = ToUint128(val)
		case "debit_account_id":
			transfer.DebitAccountID = ToUint128(val)
		case "credit_account_id":
			transfer.CreditAccountID = ToUint128(val)
		case "ledger":
			transfer.Ledger = uint32(val)
		case "code":
			transfer.Code = uint16(val)
		default:
			return Transfer{}, fmt.Errorf("unknown field: %s", key)
		}
	}
	return transfer, nil
}

func loadTransfers(filename string) ([]Transfer, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var transfers []Transfer
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		transfer, err := parseLine(line)
		if err != nil {
			return nil, fmt.Errorf("error parsing line '%s': %v", line, err)
		}
		transfers = append(transfers, transfer)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading from file: %v", err)
	}
	return transfers, nil
}

func assert(condition bool, message string) {
	if !condition {
		log.Fatalf("Assertion failed: %s", message)
	}
}

var port = "3000"

func main() {
	client, err := tigerbeetle_go.NewClient(ToUint128(0), []string{port}, 8192)
	if err != nil {
		log.Fatalf("Error creating TigerBeetle client: %v", err)
	} else {
		log.Println("Connected to Tigerbeetle client")
	}
	defer client.Close()

	// Create two accounts
	res, err := client.CreateAccounts([]Account{
		{
			ID:     ToUint128(1),
			Ledger: 1,
			Code:   1,
			Flags:  AccountFlags{DebitsMustNotExceedCredits: true}.ToUint16(),
		},
		{
			ID:     ToUint128(2),
			Ledger: 1,
			Code:   1,
			Flags:  AccountFlags{DebitsMustNotExceedCredits: true}.ToUint16(),
		},
		{
			ID:     ToUint128(3),
			Ledger: 1,
			Code:   1,
			Flags:  AccountFlags{CreditsMustNotExceedDebits: true}.ToUint16(),
		},
		{
			ID:     ToUint128(4),
			Ledger: 1,
			Code:   1,
			Flags:  AccountFlags{CreditsMustNotExceedDebits: true}.ToUint16(),
		},
		{
			ID:     ToUint128(7000000),
			Ledger: 1,
			Code:   1,
		},
		{
			ID:     ToUint128(7000001),
			Ledger: 1,
			Code:   1,
		},
	})
	if err != nil {
		log.Fatalf("Error creating accounts: %s", err)
	} else {
		log.Println("Test Accounts created")
	}

	for _, err := range res {
		log.Fatalf("Error creating account %d: %s", err.Index, err.Result)
	}

	// Check the sums for both accounts
	_accounts, err := client.LookupAccounts([]Uint128{ToUint128(1), ToUint128(2), ToUint128(3), ToUint128(4)})
	if err != nil {
		log.Fatalf("Could not fetch accounts: %s", err)
	}
	assert(len(_accounts) == 4, "accounts")
	fmt.Printf("Test Accounts status before load start %+v\n\n", _accounts)

	log.Printf("Started loading transfers...:\n\n")

	// Load transfers from JSON
	transfers, err := loadTransfers("transfers.txt")
	if err != nil {
		log.Fatalf("Error loading transfers: %s", err)
	} else {
		log.Printf("Loaded 5 million transfers...\n\n")
	}

	log.Printf("Started batch transferring...:\n\n")
	startTime := time.Now()

	// Process transfers in batches
	const BATCH_SIZE = 8190
	for i := 0; i < len(transfers); i += BATCH_SIZE {
		batch := BATCH_SIZE
		if i+BATCH_SIZE > len(transfers) {
			batch = len(transfers) - i
		}
		_, err = client.CreateTransfers(transfers[i : i+batch])
		if err != nil {
			log.Fatalf("Error creating transfers: %s\n", err)
		}

	}

	log.Printf("Finished batch transferring, Now fetching accounts: \n\n")

	// Check the sums for both accounts
	accounts, err := client.LookupAccounts([]Uint128{ToUint128(1), ToUint128(2), ToUint128(3), ToUint128(4)})
	if err != nil {
		log.Fatalf("Could not fetch accounts: %s", err)
	}
	assert(len(accounts) == 4, "accounts")

	fmt.Printf("Time taken: %v\n", time.Since(startTime))

	fmt.Printf("Test Accounts after batch transferring...: %+v\n", accounts)

}
