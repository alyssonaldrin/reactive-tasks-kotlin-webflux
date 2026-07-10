package com.alyssonaldrin.learning.reactive_tasks.task

import org.springframework.stereotype.Component
import reactor.core.publisher.Flux
import reactor.core.publisher.Sinks

@Component
class TaskEventPublisher {

    private val sink = Sinks.many().multicast().onBackpressureBuffer<Task>(256, false)

    fun publish(task: Task) {
        sink.tryEmitNext(task)
    }

    fun stream(): Flux<Task> = sink.asFlux()
}