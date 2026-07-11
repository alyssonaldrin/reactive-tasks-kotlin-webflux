# Diário de Aprendizado — Reactive Tasks

Registro pessoal do que fui entendendo ao longo do projeto. A ideia é anotar o
"clique" de cada conceito, não só a teoria pronta.

---

## Fase 0 — Setup

- Projeto criado com Spring Initializr: **Spring Boot 4.0**, **Kotlin**, **Gradle (Kotlin DSL)**, **Java 25**.
- Dependências: Spring Reactive Web (WebFlux), Spring Data R2DBC, driver PostgreSQL, Validation, DevTools.
- Formato de configuração escolhido: **YAML** em vez de `.properties` — mais legível
  pra estrutura hierárquica, e já adianta a sintaxe que vou usar depois no Kubernetes.
- Configurei **Reformat on Save** no IntelliJ (Settings → Tools → Actions on Save),
  usando o estilo oficial do Kotlin.

---

## Fase 1 — Fundamentos reativos

### `Mono` e `Flux` são "receitas", não valores prontos

O maior insight aqui: criar um `Mono` ou `Flux` **não executa nada**. É só uma
descrição do que vai acontecer. Nada roda até alguém chamar `.subscribe()`.

Testei isso na prática:

```kotlin
val numeros = Flux.just(1, 2, 3, 4, 5)
    .map {
        println("Mapeando: $it")
        it * 2
    }

println("Criei o Flux, mas ainda não rodei nada")

numeros.subscribe { n -> println("Processado: $n") }
```

**Resultado no console:**

```
Criei o Flux, mas ainda não rodei nada
Mapeando: 1
Processado: 2
Mapeando: 2
Processado: 4
...
```

### Duas descobertas importantes

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

## Fase 2 — CRUD reativo e streaming (SSE)

- Criados `Task` (entidade), `TaskRepository` (Spring Data R2DBC),
  `TaskService` (regras de negócio) e `TaskController` (endpoints REST).
- Aprendido o uso de `flatMap` (quando a transformação retorna outro
  `Mono`/`Flux`) vs `map` (quando retorna um valor comum) — usado em
  `toggleCompleted`, que busca a tarefa e depois salva a atualização.
- Implementado endpoint de streaming em tempo real via **SSE**
  (`GET /api/tasks/stream`), usando `Sinks.many().multicast()` para
  distribuir eventos de criação/atualização de tarefas para clientes
  conectados.
- Introduzido `doOnNext` — efeito colateral que não transforma o valor,
  usado para publicar eventos sem acoplar `TaskService` à lógica de streaming.

### Bug: conexão SSE fechando instantaneamente

Depois de implementar o endpoint de stream, todas as conexões (curl, navegador,
Postman) fechavam na hora, sem erro aparente.

**Causa:** por padrão, `onBackpressureBuffer()` usa `autoCancel = true`. Isso
significa que assim que o último assinante se desconecta, o Sink se marca como
encerrado *para sempre* — qualquer conexão nova depois disso recebe um sinal de
"já terminou" instantaneamente.

**Correção:** desativar o autoCancel explicitamente:

```kotlin
Sinks.many().multicast().onBackpressureBuffer<Task>(256, false)
```

**Lição:** em programação reativa, o *ciclo de vida* dos publishers (quando eles
começam, terminam, ou se resetam) é tão importante quanto a lógica de
transformação dos dados. Um Sink não é só uma fila — ele tem estados (ativo,
completo, com erro) que afetam todo mundo conectado nele.

### `curl` no PowerShell não é o curl de verdade

No Windows, `curl` dentro do PowerShell é um alias para `Invoke-WebRequest`,
que tem sintaxe diferente e não lida bem com streaming. Solução: chamar
explicitamente `curl.exe` para usar o binário real.

---

## Fase 3 — Testes reativos

### `StepVerifier`: testando `Mono`/`Flux` isoladamente

Testei `TaskService` sem depender de banco real, usando mocks (`mockito-kotlin`)
para `TaskRepository` e `TaskEventPublisher`. O `StepVerifier` cumpre, nos
testes, o mesmo papel do `.subscribe()`: sem chamar um método terminal
(`.verifyComplete()`, `.verifyError()`), nada é executado — mesmo princípio de
lazy evaluation da Fase 1, agora aplicado a testes.

Padrão usado:

```kotlin
StepVerifier.create(service.toggleCompleted(1L))
    .expectNextMatches { it.completed }
    .verifyComplete()
```

Também testei o caminho "vazio" (`Mono.empty()` quando a tarefa não existe) —
um `Mono` vazio que completa sem erro é, em si, um resultado válido a testar.

### `WebTestClient`: testando endpoints de ponta a ponta

Usei `@WebFluxTest(TaskController::class)` para subir só a camada web (sem
banco real), com `WebTestClient` simulando requisições HTTP de verdade contra
o Controller. Para o endpoint de streaming, testei apenas que a conexão abre
com o `Content-Type: text/event-stream` correto — validar o conteúdo do stream
em si seria mais teste de integração, e o `Flux` de SSE nunca termina sozinho
(reflexo direto do bug do `autoCancel` que corrigi na Fase 2).

### `@MockBean` foi removido no Spring Boot 4.0

Ao escrever os testes com `WebTestClient`, o import de `@MockBean`
(`org.springframework.boot.test.mock.mockito.MockBean`) não resolveu. Motivo:
essa anotação foi **removida** no Spring Boot 4.0 (estava deprecated desde a
3.4), substituída por `@MockitoBean`, agora parte do **Spring Framework**
(`org.springframework.test.context.bean.override.mockito.MockitoBean`), não
mais do Spring Boot.

### `@WebFluxTest` também mudou de pacote no Boot 4.0

- Antes (3.x): `org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest`
- Agora (4.0): `org.springframework.boot.webflux.test.autoconfigure.WebFluxTest`

O Boot 4.0 reorganizou os pacotes de auto-configuração de teste agrupando por
tecnologia (`webflux`, `webmvc`, etc), em vez da estrutura antiga baseada em
`web.reactive`/`web.servlet`.

**Lição geral da fase:** ao usar uma versão muito recente de um framework
(Spring Boot 4.0 é major release novo), boa parte do conteúdo encontrado em
buscas e tutoriais antigos está defasado. Deixar o IntelliJ sugerir o import
correto (Alt+Enter) e checar a documentação oficial da versão exata em uso é
mais confiável do que copiar exemplos prontos.

### JaCoCo não suportava Java 25 (versão 0.8.12)

Ao rodar `jacocoTestReport`, o build falhou tentando instrumentar até classes
internas do JDK (`sun.security.*`), com o erro `Unsupported class file major
version 69` — 69 é o major version do bytecode do **Java 25**.

**Causa:** JaCoCo `0.8.12` não tinha suporte oficial a Java 25. O suporte
oficial só chegou na versão `0.8.14`.

**Correção:** atualizar `toolVersion` no bloco `jacoco { }` para `"0.8.14"`.

**Lição:** ao usar a versão mais recente de uma linguagem (JDK 25, LTS
recém-lançado), quase toda ferramenta do ecossistema (coverage, análise
estática, linters) pode estar um passo atrás em compatibilidade. Vale sempre
checar o changelog oficial da ferramenta antes de assumir que "deveria
funcionar".

### Gradle Daemon não recarrega `gradle.properties` sozinho

Depois de mover `org.gradle.java.home` para o `gradle.properties` global
(`~/.gradle/gradle.properties`), o build continuou falhando como se o arquivo
não existisse. Causa: o Gradle Daemon (processo em background que acelera
builds) mantém configuração em memória e não relê arquivos de properties
automaticamente. Solução: `./gradlew --stop` força o encerramento do daemon,
e a próxima execução recarrega tudo do zero.

---

## Fase 4 — Containerização

- Criado Dockerfile multi-stage: etapa `build` (JDK completo, compila o `.jar`)
  e etapa `runtime` (só o JRE, roda o `.jar` final) — imagem final bem mais
  enxuta que carregar o JDK completo em produção.
- Usuário não-root (`spring`) criado na imagem por segurança.

### Containers não compartilham "localhost" entre si

Rodando `docker run` isolado, a aplicação não conseguia conectar no Postgres
(`Cannot connect to localhost:5432`), mesmo com o Postgres rodando em outro
container. Causa: cada container tem seu próprio "localhost" isolado — eles
não se enxergam por padrão.

**Correção:** adicionar a aplicação como serviço no próprio `docker-compose.yml`,
usando o **nome do serviço** (`postgres`) como hostname em vez de `localhost`
(`r2dbc:postgresql://postgres:5432/tasks`). O Compose cria uma rede interna
onde os serviços se resolvem pelo nome.

**Lição:** "localhost" dentro de um container não é a máquina host nem outros
containers — é o próprio container. Comunicação entre containers precisa
passar pela rede que os orquestra (Compose, ou depois, Kubernetes).

### `healthcheck` evita corrida entre containers

Sem um healthcheck no Postgres, a aplicação às vezes tentava conectar antes do
banco estar realmente pronto para aceitar conexões (mesmo já "rodando").
`depends_on: condition: service_healthy` resolve isso, esperando o Postgres
responder a `pg_isready` antes de liberar a aplicação para iniciar.
---

## Perguntas em aberto / pra revisitar depois

- Como exatamente o backpressure funciona na prática em cenários de alta carga
  (ainda só entendi o conceito e o parâmetro de buffer do Sink)?
- Diferença entre `subscribe()` manual e deixar o próprio WebFlux gerenciar a
  subscription (via retorno do Controller)?
- Vale a pena testar o conteúdo real do stream SSE (não só o Content-Type) em
  um teste de integração mais completo?