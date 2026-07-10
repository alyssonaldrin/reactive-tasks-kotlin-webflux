package com.alyssonaldrin.learning.reactive_tasks.task

import org.springframework.data.repository.reactive.ReactiveCrudRepository
import org.springframework.stereotype.Repository
import reactor.core.publisher.Flux

@Repository
interface TaskRepository : ReactiveCrudRepository<Task, Long> {

    fun findAllByCompleted(completed: Boolean): Flux<Task>
}