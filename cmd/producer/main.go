package main

import (
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	deliveryChan := make(chan kafka.Event)
	producer := NewKafkaProducer()
	Publish("transferiu", "teste", producer, []byte("transferencia2"), deliveryChan)
	go DeliveryReport(deliveryChan) //async, roda em uma nova thread
	producer.Flush(1000)            //espera receber esse retorno
}

func NewKafkaProducer() *kafka.Producer {
	//É um mapa de config que passamos para o nosso producer
	configMap := &kafka.ConfigMap{
		"bootstrap.servers":   "kafka-go_kafka_1:9092",
		"delivery.timeout.ms": "0",     //tempo max de entrega de uma msg
		"acks":                "all",   //0, 1, all
		"enable.idempotence":  "false", //O produtor não é idempotente
	}

	producer, err := kafka.NewProducer(configMap)
	if err != nil {
		log.Println(err.Error())
	}
	return producer
}

func Publish(msg string, topic string, producer *kafka.Producer, key []byte, deliveryChan chan kafka.Event) error {
	message := &kafka.Message{
		Value:          []byte(msg),
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            key,
	}
	err := producer.Produce(message, deliveryChan)
	if err != nil {
		return err
	}
	return nil
}

func DeliveryReport(deliveryChan chan kafka.Event) {
	for e := range deliveryChan {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				fmt.Println("Erro ao enviar")
			} else {
				fmt.Println("Mesagem enviada: ", ev.TopicPartition)
				//anotar no banco de dados que a mensagem foi processada.
				//confirma que uma transferencia bancaria ocorreu
			}

		}
	}
}
