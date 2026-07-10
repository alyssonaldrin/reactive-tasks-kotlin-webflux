package com.alyssonaldrin.learning.reactive_tasks.task

import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

@Service
class TaskService(
    private val repository: TaskRepository,
    private val eventPublisher: TaskEventPublisher
) {

    fun findAll(): Flux<Task> = repository.findAll()

    fun findById(id: Long): Mono<Task> = repository.findById(id)

    fun create(title: String): Mono<Task> {
        val task = Task(title = title)
        return repository.save(task)
            .doOnNext { saved -> eventPublisher.publish(saved) }
    }

    fun toggleCompleted(id: Long): Mono<Task> {
        return repository.findById(id)
            .flatMap { task -> repository.save(task.copy(completed = !task.completed)) }
            .doOnNext { updated -> eventPublisher.publish(updated) }
    }

    fun delete(id: Long): Mono<Void> = repository.deleteById(id)
}