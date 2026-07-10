# Diário de Aprendizado — Reactive Tasks

Registro pessoal do que fui entendendo ao longo do projeto. A ideia é anotar o
"clique" de cada conceito, não só a teoria pronta.

---

## Fase 0 — Setup

- Projeto criado com Spring Initializr: **Spring Boot 4.0**, **Kotlin**, **Gradle (Kotlin DSL)**, **Java 25**.
- Dependências: Spring Reactive Web (WebFlux), Spring Data R2DBC, driver PostgreSQL, Validation, DevTools.
- Formato de configuração escolhido: **YAML** em vez de `.properties` — mais legível
  pra estrutura hierárquica, e já adianta a sintaxe que vou usar depois no Kubernetes.

---

## Fase 1 — Fundamentos reativos

### `Mono` e `Flux` são "receitas", não valores prontos

O maior insight aqui: criar um `Mono` ou `Flux` **não executa nada**. É só uma
descrição do que vai acontecer. Nada roda até alguém chamar `.subscribe()`.

Testei isso na prática:

\`\`\`kotlin
val numeros = Flux.just(1, 2, 3, 4, 5)
.map {
println("Mapeando: $it")
it * 2
}

println("Criei o Flux, mas ainda não rodei nada")

numeros.subscribe { n -> println("Processado: $n") }
\`\`\`

**Resultado no console:**
\`\`\`
Criei o Flux, mas ainda não rodei nada
Mapeando: 1
Processado: 2
Mapeando: 2
Processado: 4
...
\`\`\`

### Duas descobertas importantes:

1. **Lazy evaluation** — o print "Criei o Flux" aparece antes de qualquer
   processamento, provando que a "receita" só roda no `.subscribe()`.
2. **Processamento item a item** — cada número passa por *todo* o pipeline
   (map → filter → subscribe) antes do próximo número começar. Diferente de uma
   `List` comum, que mapearia tudo de uma vez e só depois filtraria tudo.

### Diferença Mono vs Flux

- `Mono<T>` → 0 ou 1 item (ex: buscar uma tarefa por ID)
- `Flux<T>` → 0 a N itens, ao longo do tempo (ex: listar tarefas, ou um stream
  contínuo de eventos via SSE)

---

## Fase 2 — Em andamento

*(vou preencher conforme avançamos: Repository, primeiro endpoint, SSE...)*

---

## Perguntas em aberto / pra revisitar depois

- Como exatamente o backpressure funciona na prática (ainda só entendi o conceito)?
- Diferença entre `subscribe()` manual e deixar o próprio WebFlux gerenciar a
  subscription (via retorno do Controller)?