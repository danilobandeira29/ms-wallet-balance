package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/danilobandeira29/ms-wallet-balance/internal/database"
	"github.com/danilobandeira29/ms-wallet-balance/internal/kafka"
	"log"
	"net/http"
)

type BalanceUpdatedDto struct {
	Payload database.Transaction `json:"Payload"`
	Name    string               `json:"Name"`
}

var repo = database.NewRepository()

func main() {
	balanceCh := make(chan database.Transaction)
	errCh := make(chan error)
	go consumerKafka(balanceCh, errCh)
	go func() {
		for {
			select {
			case err := <-errCh:
				log.Println(err)
			}
		}
	}()
	go saveInDatabase(repo, balanceCh)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /balances/{accountId}", balanceHandler)
	fmt.Println("server listening at port http://localhost:8080")
	log.Fatalln(http.ListenAndServe(":8080", mux))
}

func saveInDatabase(repository *database.Repository, ch <-chan database.Transaction) {
	for {
		select {
		case transaction := <-ch:
			err := repository.Save(transaction)
			if err != nil {
				log.Printf("savetransaction: %v", err)
			}
		}
	}
}

func consumerKafka(ch chan<- database.Transaction, errCh chan<- error) {
	for {
		mgs, err := kafka.Consumer.ReadMessage(-1)
		if err != nil {
			errCh <- fmt.Errorf("error when reading message %v", err)
			continue
		}
		var balanceEvent *BalanceUpdatedDto
		err = json.Unmarshal(mgs.Value, &balanceEvent)
		if err != nil {
			errCh <- fmt.Errorf("error when unmarshal kafka's topic %v", err)
			continue
		}
		ch <- balanceEvent.Payload
	}
}

func balanceHandler(w http.ResponseWriter, r *http.Request) {
	accountId := r.PathValue("accountId")
	w.Header().Set("Content-Type", "application/json")
	if accountId == "" {
		w.WriteHeader(http.StatusNotFound)
		_, err := w.Write([]byte("{\"message\": \"accountId is mandatory\"}"))
		if err != nil {
			log.Printf("findone: error when send validation error to the client %v\n", err)
		}
		return
	}
	balance, err := repo.FindOneByAccountId(accountId)
	if err != nil {
		switch {
		case errors.Is(err, database.ErrNoRows):
			w.WriteHeader(http.StatusNotFound)
			_, err = w.Write([]byte("{\"data\": null }"))
			if err != nil {
				log.Printf("findone: error when trying to send empty balance to the client %v", err)
			}
			return
		default:
			log.Printf("findone: %v\n", err)
			w.WriteHeader(http.StatusInternalServerError)
			_, err := w.Write([]byte("{\"message\": \"internal server error\"}"))
			if err != nil {
				log.Printf("findone: error when send internal server error message to the client %v\n", err)
			}
			return
		}
	}
	w.WriteHeader(http.StatusOK)
	_, err = w.Write([]byte(fmt.Sprintf("{\"data\": {\"account_id\": \"%s\", \"balance\": %d}}", balance.AccountId, balance.Value)))
	if err != nil {
		log.Printf("findone: error when trying to send balance to the client %v", err)
	}
}
