package com.alyssonaldrin.learning.reactive_tasks

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class ReactiveTasksApplication

fun main(args: Array<String>) {
    runApplication<ReactiveTasksApplication>(*args)
}