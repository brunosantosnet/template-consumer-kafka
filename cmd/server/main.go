package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/segmentio/kafka-go"
)

const (
	insertOffsets = `insert into db.t1 (_offset, _partition, _key, _value) values (?, ?, ?, ?)`
)

func main() {

	topics := strings.Split(os.Getenv("TOPICS"), ",")

	for _, v := range topics {
		c := fmt.Sprintf("consumer-%s", v)
		go consumer(v, c)
	}

	time.Sleep(300 * time.Second)
	fmt.Println("Encerrando threads")

}

func consumer(topic string, consumer string) {

	fmt.Println("Consumindo topic : [", topic, "]")
	db, err := sqlx.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:3306)/db", os.Getenv("USER"), os.Getenv("PASS"), os.Getenv("ENDPOINT")))
	if err = db.Ping(); err != nil {
		msg := "error: " + err.Error()
		log.Fatal(msg)
	}
	defer db.Close()

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		GroupID: consumer,
		Topic:   topic,
	})

	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			break
		}
		tx := db.MustBegin()
		tx.MustExec(insertOffsets, m.Offset, m.Partition, m.Key, m.Value)
		tx.Commit()
		fmt.Printf("Mensagem de [%s] transferida\n", topic)
	}

	r.Close()

}
