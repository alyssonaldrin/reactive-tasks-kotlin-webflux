package com.alyssonaldrin.learning.reactive_tasks.task

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import reactor.core.publisher.Flux
import reactor.kafka.receiver.KafkaReceiver
import reactor.kafka.receiver.ReceiverOptions
import reactor.kafka.sender.KafkaSender
import reactor.kafka.sender.SenderOptions
import reactor.kafka.sender.SenderRecord
import tools.jackson.databind.json.JsonMapper

@Component
class TaskEventPublisher(
    @Value("\${app.kafka.bootstrap-servers}") bootstrapServers: String,
    @Value("\${app.kafka.topics.task-events:task-events}") private val topic: String
) {

    private val objectMapper = JsonMapper.builder().findAndAddModules().build()

    // Producer reativo: publica eventos no tópico do Kafka
    private val sender: KafkaSender<String, String> = KafkaSender.create(
        SenderOptions.create<String, String>(
            mapOf(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java
            )
        )
    )

    // Opções do consumer reativo, usadas para criar um KafkaReceiver por conexão SSE
    private val bootstrapServersConfig = bootstrapServers

    fun publish(task: Task) {
        val json = objectMapper.writeValueAsString(task)
        val record = SenderRecord.create(
            ProducerRecord(topic, task.id.toString(), json),
            null
        )
        sender.send(Flux.just(record)).subscribe()
    }

    fun stream(): Flux<Task> {
        // Cada nova conexão SSE ganha seu próprio "grupo" de consumo, começando a
        // ler só eventos NOVOS (latest) — evita reprocessar todo o histórico do
        // tópico a cada cliente que conecta.
        val receiverOptions = ReceiverOptions.create<String, String>(
            mapOf(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServersConfig,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
                ConsumerConfig.GROUP_ID_CONFIG to "sse-stream-${System.nanoTime()}",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "latest"
            )
        ).subscription(listOf(topic))

        return KafkaReceiver.create(receiverOptions)
            .receive()
            .map { record -> objectMapper.readValue(record.value(), Task::class.java) }
    }
}