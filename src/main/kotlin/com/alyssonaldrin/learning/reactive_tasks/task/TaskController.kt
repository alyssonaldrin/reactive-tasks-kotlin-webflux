package com.alyssonaldrin.learning.reactive_tasks.task

import org.springframework.web.bind.annotation.*
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

@RestController
@RequestMapping("/api/tasks")
class TaskController(
    private val service: TaskService
) {

    @GetMapping
    fun findAll(): Flux<Task> = service.findAll()

    @GetMapping("/{id}")
    fun findById(@PathVariable id: Long): Mono<Task> = service.findById(id)

    @PostMapping
    fun create(@RequestBody request: CreateTaskRequest): Mono<Task> =
        service.create(request.title)

    @PatchMapping("/{id}/toggle")
    fun toggleCompleted(@PathVariable id: Long): Mono<Task> =
        service.toggleCompleted(id)

    @DeleteMapping("/{id}")
    fun delete(@PathVariable id: Long): Mono<Void> = service.delete(id)
}

data class CreateTaskRequest(val title: String)