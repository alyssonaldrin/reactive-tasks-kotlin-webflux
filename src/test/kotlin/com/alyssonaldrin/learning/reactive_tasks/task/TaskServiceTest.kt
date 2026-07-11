package com.alyssonaldrin.learning.reactive_tasks.task

import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import reactor.core.publisher.Mono
import reactor.test.StepVerifier

class TaskServiceTest {

    private val repository: TaskRepository = mock()
    private val eventPublisher: TaskEventPublisher = mock()
    private val service = TaskService(repository, eventPublisher)

    @Test
    fun `create deve salvar a tarefa e publicar o evento`() {
        val novaTask = Task(title = "Aprender StepVerifier")
        val taskSalva = novaTask.copy(id = 1L)

        whenever(repository.save(any())).thenReturn(Mono.just(taskSalva))

        StepVerifier.create(service.create("Aprender StepVerifier"))
            .expectNextMatches { it.id == 1L && it.title == "Aprender StepVerifier" }
            .verifyComplete()

        verify(eventPublisher).publish(taskSalva)
    }

    @Test
    fun `toggleCompleted deve inverter o status da tarefa`() {
        val taskExistente = Task(id = 1L, title = "Tarefa X", completed = false)
        val taskAtualizada = taskExistente.copy(completed = true)

        whenever(repository.findById(1L)).thenReturn(Mono.just(taskExistente))
        whenever(repository.save(any())).thenReturn(Mono.just(taskAtualizada))

        StepVerifier.create(service.toggleCompleted(1L))
            .expectNextMatches { it.completed }
            .verifyComplete()
    }

    @Test
    fun `toggleCompleted deve completar vazio quando tarefa nao existe`() {
        whenever(repository.findById(99L)).thenReturn(Mono.empty())

        StepVerifier.create(service.toggleCompleted(99L))
            .verifyComplete()
    }
}

private fun <T> any(): T = org.mockito.kotlin.any()