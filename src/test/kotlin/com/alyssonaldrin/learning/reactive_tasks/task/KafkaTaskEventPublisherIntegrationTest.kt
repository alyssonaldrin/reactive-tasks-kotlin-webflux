package com.alyssonaldrin.learning.reactive_tasks.task

import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.kafka.KafkaContainer
import reactor.test.StepVerifier
import java.time.Duration

@Testcontainers
class KafkaTaskEventPublisherIntegrationTest {

    companion object {
        // Container Kafka real (mesma imagem usada no docker-compose.yml),
        // criado uma única vez e compartilhado entre os testes desta classe.
        @JvmStatic
        private val kafkaContainer = KafkaContainer("apache/kafka-native:3.8.0")

        private lateinit var publisher: TaskEventPublisher

        @JvmStatic
        @BeforeAll
        fun setUp() {
            kafkaContainer.start()
            publisher = KafkaTaskEventPublisher(
                bootstrapServers = kafkaContainer.bootstrapServers,
                topic = "task-events-test"
            )
        }

        @JvmStatic
        @AfterAll
        fun tearDown() {
            kafkaContainer.stop()
        }
    }

    @Test
    fun `deve publicar e receber uma tarefa via Kafka real`() {
        val task = Task(id = 1L, title = "Testar Kafka com Testcontainers")

        // Começa a "ouvir" o stream ANTES de publicar, senão a mensagem
        // poderia ser publicada antes de existir alguém escutando.
        val eventos = publisher.stream().take(1)

        StepVerifier.create(eventos)
            .thenAwait(Duration.ofSeconds(2))
            .then { publisher.publish(task) }
            .expectNextMatches { received ->
                received.id == task.id && received.title == task.title
            }
            .expectComplete()
            .verify(Duration.ofSeconds(15))
    }

    @Test
    fun `deve publicar múltiplas tarefas e recebê-las em ordem`() {
        val task1 = Task(id = 10L, title = "Primeira")
        val task2 = Task(id = 11L, title = "Segunda")

        val eventos = publisher.stream().take(2)

        StepVerifier.create(eventos)
            .thenAwait(Duration.ofSeconds(2))
            .then { publisher.publish(task1) }
            .then { publisher.publish(task2) }
            .expectNextMatches { it.id == 10L }
            .expectNextMatches { it.id == 11L }
            .expectComplete()
            .verify(Duration.ofSeconds(15))
    }
}