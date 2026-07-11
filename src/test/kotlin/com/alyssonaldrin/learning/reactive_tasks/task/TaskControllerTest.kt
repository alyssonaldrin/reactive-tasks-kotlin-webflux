package com.alyssonaldrin.learning.reactive_tasks.task

import org.junit.jupiter.api.Test
import org.mockito.kotlin.whenever
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.webflux.test.autoconfigure.WebFluxTest
import org.springframework.http.MediaType
import org.springframework.test.context.bean.override.mockito.MockitoBean
import org.springframework.test.web.reactive.server.WebTestClient
import org.springframework.test.web.reactive.server.expectBodyList
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

@WebFluxTest(TaskController::class)
class TaskControllerTest {

    @Autowired
    lateinit var webTestClient: WebTestClient

    @MockitoBean
    lateinit var service: TaskService

    @MockitoBean
    lateinit var eventPublisher: TaskEventPublisher

    @Test
    fun `GET api-tasks deve retornar lista de tarefas`() {
        val tasks = listOf(
            Task(id = 1L, title = "Tarefa 1"),
            Task(id = 2L, title = "Tarefa 2")
        )
        whenever(service.findAll()).thenReturn(Flux.fromIterable(tasks))

        webTestClient.get()
            .uri("/api/tasks")
            .exchange()
            .expectStatus().isOk
            .expectBodyList<Task>()
            .hasSize(2)
    }

    @Test
    fun `POST api-tasks deve criar uma tarefa e retornar 200 com o corpo`() {
        val request = CreateTaskRequest(title = "Nova tarefa")
        val taskCriada = Task(id = 1L, title = "Nova tarefa")

        whenever(service.create("Nova tarefa")).thenReturn(Mono.just(taskCriada))

        webTestClient.post()
            .uri("/api/tasks")
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(request)
            .exchange()
            .expectStatus().isOk
            .expectBody()
            .jsonPath("$.id").isEqualTo(1)
            .jsonPath("$.title").isEqualTo("Nova tarefa")
    }

    @Test
    fun `GET api-tasks stream deve responder com content-type text-event-stream`() {
        whenever(eventPublisher.stream()).thenReturn(Flux.empty())

        webTestClient.get()
            .uri("/api/tasks/stream")
            .accept(MediaType.TEXT_EVENT_STREAM)
            .exchange()
            .expectStatus().isOk
            .expectHeader().contentTypeCompatibleWith(MediaType.TEXT_EVENT_STREAM)
    }
}