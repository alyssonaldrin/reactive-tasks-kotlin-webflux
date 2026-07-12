package com.alyssonaldrin.learning.reactive_tasks.task

import reactor.core.publisher.Flux

/**
 * Contrato para publicação e streaming de eventos de tarefas.
 *
 * Hoje a única implementação é [KafkaTaskEventPublisher], mas manter esse
 * contrato separado permite trocar a tecnologia por trás (ex: RabbitMQ, ou
 * até voltar para Sinks em memória em testes) sem alterar TaskService nem
 * TaskController, que dependem apenas desta interface.
 */
interface TaskEventPublisher {

    fun publish(task: Task)

    fun stream(): Flux<Task>
}