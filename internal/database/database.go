package database

import (
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"os"
	"sync"
)

type Repository struct {
	db *sql.DB
}

type Transaction struct {
	AccountIDFrom      string `json:"account_id_from"`
	AccountIDTo        string `json:"account_id_to"`
	BalanceAccountFrom int64  `json:"balance_account_from"`
	BalanceAccountTo   int64  `json:"balance_account_to"`
}

type Balance struct {
	Value     int64  `json:"value"`
	AccountId string `json:"account_id"`
}

const createBalanceTable = `
	create table if not exists balances (
		account_id varchar(255) primary key,
		value int not null
	)`

var (
	ErrSave      = errors.New("error when trying to save")
	ErrFindByOne = errors.New("error when query row")
	ErrNoRows    = sql.ErrNoRows
	once         sync.Once
)

func NewRepository() *Repository {
	repo := &Repository{}
	var db *sql.DB
	once.Do(func() {
		var err error
		db, err = sql.Open(
			"mysql",
			fmt.Sprintf("%s:%s@tcp(mysqlbalance:%s)/balance?parseTime=true",
				os.Getenv("BALANCE_MYSQL_USER"),
				os.Getenv("BALANCE_MYSQL_PASSWORD"),
				os.Getenv("BALANCE_MYSQL_PORT"),
			))
		if err != nil {
			log.Fatalf("cannot connect to balance's database %v\n", err)
		}
		_, err = db.Exec(createBalanceTable)
		if err != nil {
			log.Fatalf("not possible to create balance table %v\n", err)
		}
	})
	repo.db = db
	return repo
}

func (r *Repository) Save(t Transaction) error {
	tx, err := r.db.Begin()
	if err != nil {
		return fmt.Errorf("%w creating transaction: %v", ErrSave, err)
	}
	stmt, err := tx.Prepare(`
			insert into balances(account_id, value)
			values (?, ?)
			on duplicate key update
			value = values(value)
		`)
	if err != nil {
		if txErr := tx.Rollback(); txErr != nil {
			return fmt.Errorf("%w rollback for balance to: %v", ErrSave, err)
		}
		return fmt.Errorf("%w prepare stmt for balance to: %v", ErrSave, err)
	}
	_, err = stmt.Exec(&t.AccountIDTo, &t.BalanceAccountTo)
	if err != nil {
		if txErr := tx.Rollback(); txErr != nil {
			return fmt.Errorf("%w exec transaction account to: %v", ErrSave, err)
		}
		return fmt.Errorf("%w exec transaction account to: %v", ErrSave, err)
	}
	stmt, err = tx.Prepare(`
			insert into balances(account_id, value)
			values (?, ?)
			on duplicate key update
			value = values(value)
		`)
	if err != nil {
		if txErr := tx.Rollback(); txErr != nil {
			return fmt.Errorf("%w rollback for balance from: %v", ErrSave, err)
		}
		return fmt.Errorf("%w  prepare stmt for balance from: %v", ErrSave, err)
	}
	_, err = stmt.Exec(&t.AccountIDFrom, &t.BalanceAccountFrom)
	if err != nil {
		if txErr := tx.Rollback(); txErr != nil {
			return fmt.Errorf("%w exec transaction account from: %v", ErrSave, err)
		}
		return fmt.Errorf("%w exec transaction account from: %v", ErrSave, err)
	}
	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("%w commiting: %v", ErrSave, err)
	}
	if err = stmt.Close(); err != nil {
		return fmt.Errorf("%w closing stmt: %v", ErrSave, err)
	}
	return nil
}

func (r *Repository) FindOneByAccountId(id string) (*Balance, error) {
	row := r.db.QueryRow(`select account_id, value from balances where account_id = ?`, &id)
	if err := row.Err(); err != nil {
		return nil, fmt.Errorf("%w query row: %v", ErrFindByOne, err)
	}
	balance := &Balance{}
	err := row.Scan(&balance.AccountId, &balance.Value)
	if err != nil {
		switch {
		case errors.Is(err, sql.ErrNoRows):
			return nil, ErrNoRows
		default:
			return nil, fmt.Errorf("%w scan: %v", ErrFindByOne, err)
		}
	}
	return balance, nil
}
