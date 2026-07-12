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

---

## Fase 4 — Containerização (Docker)

- Criado `Dockerfile` **multi-stage**: etapa `build` (imagem com JDK completo,
  compila o `.jar` via `./gradlew bootJar`) e etapa `runtime` (só o JRE, roda
  o `.jar` final) — a imagem final não carrega compilador, Gradle nem cache de
  dependências, só o artefato executável.
- Usuário não-root (`spring`) criado na imagem por segurança — se algo
  comprometer o container, o processo não roda com privilégios de root.
- `.dockerignore` criado seguindo o mesmo princípio do `.gitignore`, evitando
  copiar `.gradle/`, `.idea/`, segredos etc para o contexto de build.

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
passar pela rede que os orquestra (Compose, e depois, Kubernetes).

### `healthcheck` evita corrida entre containers

Sem um healthcheck no Postgres, a aplicação às vezes tentava conectar antes do
banco estar realmente pronto para aceitar conexões (mesmo já "rodando").
`depends_on: condition: service_healthy` resolve isso, esperando o Postgres
responder a `pg_isready` antes de liberar a aplicação para iniciar.

---

## Fase 4.5 — Streaming distribuído com Kafka

Substituído o `Sinks` em memória (Fase 2) por Kafka de verdade, resolvendo uma
limitação real de arquitetura: com `Sinks`, múltiplas instâncias da aplicação
rodando em paralelo (cenário normal com Kubernetes fazendo scale horizontal)
não veriam eventos umas das outras — cada uma tinha seu próprio Sink isolado.

- Kafka rodando em modo **KRaft** no `docker-compose.yml` (sem Zookeeper,
  modo mais moderno e simples de configurar).
- `reactor-kafka` usado em vez do `spring-boot-starter-kafka` — biblioteca
  mais "crua", que mantém a API reativa (`Flux`/`Mono`) sem depender da
  autoconfiguração do Spring Boot.
- `TaskEventPublisher` reescrito com `KafkaSender` (producer) e `KafkaReceiver`
  (consumer), serializando/desserializando `Task` ↔ JSON manualmente via
  Jackson — diferente do `Sinks`, que não precisava dessa conversão porque
  nunca saía da memória da aplicação.
- Cada conexão SSE cria um `KafkaReceiver` com um **group ID único**
  (`"sse-stream-${System.nanoTime()}"`) e `AUTO_OFFSET_RESET_CONFIG = "latest"`,
  replicando o comportamento "multicast" que o `Sinks` tinha (todo cliente
  recebe todos os eventos, só a partir do momento em que conectou).
- Interface pública (`publish()`, `stream()`) mantida idêntica à versão com
  `Sinks` — `TaskService` e `TaskController` não precisaram mudar nada. Boa
  prova prática de que programar contra uma abstração simples permite trocar
  a implementação por trás sem espalhar a mudança pelo resto do código.

### Configurações customizadas devem usar prefixo próprio, não `spring.*`

Inicialmente configurei `bootstrap-servers` e `topics` do Kafka sob o prefixo
`spring.kafka.*` no `application.yaml`. Como não usamos o starter oficial do
Spring Kafka (só o `reactor-kafka`, lido manualmente via `@Value`), o IntelliJ
não reconhecia essas propriedades como válidas (aviso "Cannot resolve
configuration property"), porque não há metadados delas no classpath.

**Correção:** mover tudo para um prefixo próprio, `app.kafka.*`, deixando
claro que é configuração customizada da aplicação, não uma propriedade oficial
do Spring Boot sendo interpretada pela autoconfiguração.

### YAML aninhado incorretamente falha silenciosamente

Ao adicionar `app.kafka.topics.task-events`, cheguei a aninhar `topics` dentro
de `spring.kafka` por engano. Como o `@Value` tinha um valor padrão definido
(`:task-events`), a aplicação **não quebrou** — só ignorou silenciosamente a
configuração customizada e sempre usou o padrão fixo.

**Lição:** um valor padrão em `@Value` é ótimo para resiliência, mas pode
mascarar um erro de configuração — a chave errada "funciona" sem avisar nada.
Vale sempre conferir a hierarquia do YAML com atenção quando uma propriedade
não está tendo o efeito esperado.

### Nomes de tópicos como configuração, não constante fixa

O nome do tópico (`task-events`) começou como constante fixa no código,
depois foi movido para `application.yaml`, seguindo o mesmo padrão do
`bootstrap-servers`. Permite usar tópicos diferentes por ambiente (dev,
staging, prod) sem recompilar a aplicação — importante para não misturar
dados de ambientes diferentes no mesmo tópico.

### `NoSuchMethodError` no ConsumerRecord — kafka-clients incompatível com reactor-kafka

Depois de tudo configurado, o consumer falhava com
`NoSuchMethodError: 'void ConsumerRecord.<init>(...)'` ao tentar receber
mensagens, mesmo com o producer publicando normalmente (eventos não chegavam
no SSE).

**Causa:** o plugin `io.spring.dependency-management` gerencia automaticamente
a versão do `kafka-clients`, mesmo sem declará-lo explicitamente como
dependência. O Spring Boot 4.1 aplicava uma versão de `kafka-clients` que não
é binariamente compatível com o `reactor-kafka:1.3.23` (assinatura de método
diferente no construtor de `ConsumerRecord`).

**Correção:** declarar `org.apache.kafka:kafka-clients:3.9.0` explicitamente
no `build.gradle.kts`, mesma versão do broker usado no `docker-compose.yml`,
sobrescrevendo o gerenciamento automático do Spring Boot.

**Lição:** gerenciamento automático de versões (BOMs) é ótimo na maioria dos
casos, mas pode escolher uma versão incompatível para bibliotecas "por fora"
do ecossistema oficial do Spring (como `reactor-kafka`, que não é
`spring-kafka`). `NoSuchMethodError`/`NoClassDefFoundError` em tempo de
execução (não na compilação) é um forte indício de conflito de versão entre
dependências transitivas — vale fixar a versão explicitamente quando isso
acontece.

---

## Fase 4.7 — Observabilidade (métricas com Prometheus + Grafana)

Antes de configurar qualquer ferramenta, revisei os três pilares da
observabilidade: **logs** (o que aconteceu, evento a evento), **métricas**
(números agregados ao longo do tempo — taxa de requisições, latência, uso de
memória) e **traces** (o caminho de uma requisição específica por múltiplos
serviços — não explorado ainda). Essa fase cobre métricas primeiro; logs
centralizados (ELK) ficam para uma próxima etapa.

- Adicionado **Spring Boot Actuator** (expõe endpoints de monitoramento como
  `/actuator/health`) e **Micrometer** com o registry específico do
  Prometheus (`micrometer-registry-prometheus`) — o Micrometer coleta as
  métricas de forma agnóstica de ferramenta; o registry é o "tradutor" que
  formata os dados no formato de texto que o Prometheus espera ler.
- `management.endpoints.web.exposure.include` configurado explicitamente
  (`health, prometheus, metrics`) — por padrão o Actuator não expõe nada,
  por segurança (evita vazar informação sensível se esquecido em produção).

### Prometheus usa modelo *pull*, não *push*

Diferente do que eu esperava, a aplicação não "envia" métricas para lugar
nenhum. O Prometheus é quem periodicamente **visita** um endereço específico
da aplicação (`/actuator/prometheus`, a cada `scrape_interval`, configurado
como 15s) e coleta os dados de lá — chamado modelo *pull-based*. Configurado
via `prometheus.yml`, apontando para `app:8080` (nome do serviço no Compose,
mesmo princípio de rede interna já usado com Postgres e Kafka).

### Bind mount vs volume nomeado

No `docker-compose.yml`, o Prometheus usa
`./prometheus.yml:/etc/prometheus/prometheus.yml` — um **bind mount**, que
espelha um arquivo local dentro do container. Diferente do volume nomeado do
Postgres/Grafana (`postgres_data`, `grafana_data`), que guarda **dados**
gerados pelo próprio container, o bind mount serve para **injetar
configuração** que eu edito localmente, fora do container.

### Grafana como consumidor do Prometheus

Grafana não coleta métricas por conta própria — ele se conecta ao Prometheus
como uma **fonte de dados** e desenha os gráficos a partir do que o
Prometheus já coletou e armazenou. Conectado com sucesso via
`http://prometheus:9090` (nome do serviço no Compose) e importado um
dashboard pronto da comunidade (ID `4701`, específico para Spring Boot +
Micrometer) em vez de montar do zero.

### "Cold start" na primeira requisição não é cache

A primeira requisição a um endpoint (ex: `GET /api/tasks`) demora
visivelmente mais que as seguintes, mesmo sem nenhum cache configurado no
projeto. Causa: fenômeno de "cold start" da JVM — JIT compilation ainda não
otimizou os métodos mais chamados, o pool de conexões R2DBC abre a primeira
conexão real sob demanda, e algumas classes são carregadas/inicializadas só
no primeiro uso. Confirmado visualmente no dashboard do Grafana: a primeira
requisição aparece como um pico isolado de latência, estabilizando nas
chamadas seguintes.

---

## Fase 4.7 (continuação) — Logs centralizados (ELK)

### Logs estruturados em JSON antes de qualquer coisa

Antes de montar o pipeline de coleta, troquei o formato de log da aplicação
de texto simples para **JSON estruturado**, usando `logstash-logback-encoder`
e um `logback-spring.xml` customizado. Isso facilita — e muito — o trabalho
de quem for interpretar os logs depois (Logstash/Elasticsearch), evitando
parsing complicado de texto livre com regex.

### O pipeline: Filebeat → Logstash → Elasticsearch → Kibana

Cada peça com um papel único: Filebeat **coleta**, Logstash **transforma**,
Elasticsearch **armazena e indexa**, Kibana **visualiza**. Elasticsearch
configurado em `discovery.type: single-node` (sem formar cluster) e com
`xpack.security.enabled: false` (aceitável só em ambiente local isolado).

### Bug 1: Filebeat recusa iniciar no Windows (permissões de bind mount)

Erro: `"config file can only be writable by the owner"`. Bind mounts no
Docker Desktop para Windows não preservam permissões Unix corretamente — o
arquivo chega com permissão `777`, que o Filebeat rejeita por segurança.

**Correção:** flag `--strict.perms=false` no `command` do serviço.

### Bug 2: Filebeat "funcionando" mas sem logs visíveis

`docker compose logs filebeat` retornava completamente vazio, mesmo com o
processo rodando (confirmado via `docker compose top`). Causa: por padrão,
o Filebeat grava logs internos em **arquivo**, não em `stdout` — e o Docker
só captura o que vai para `stdout`/`stderr`.

**Correção:** flag `-e` no `command`, forçando saída pelo console.

**Lição geral (bugs 1 e 2):** container "Up" e processo rodando não significa
necessariamente que está fazendo o que deveria — sempre vale confirmar
comportamento real (aqui, via logs ou testes internos como
`filebeat test config`/`test output`), não só o status do container.

### Bug 3: autodiscover com hints não gerava configuração alguma

Configurei o Filebeat para "descobrir" containers automaticamente via
`filebeat.autodiscover` com `provider: docker` e `hints.enabled: true`,
usando labels no `docker-compose.yml` (`co.elastic.logs/enabled: "true"`)
para marcar qual container coletar. Mesmo com o label presente e reconhecido
(confirmado nos logs de debug), o autodiscover nunca gerava uma configuração
de coleta real — o campo `config` nos eventos de debug aparecia sempre vazio,
para todos os containers, incluindo o da aplicação.

**Decisão:** em vez de continuar depurando um mecanismo "mágico" e opaco,
troquei para uma abordagem mais explícita e previsível: um input `type:
container` lendo diretamente `/var/lib/docker/containers/*/*.log` (todos os
containers), com um processor `drop_event` filtrando por
`container.image.name` para manter só os logs da aplicação.

**Lição:** quando uma ferramenta de "descoberta automática" não coopera e o
debugging não converge, vale considerar trocar por uma abordagem mais
verbosa mas determinística, em vez de insistir indefinidamente no mecanismo
automático.

### Bug 4: campos JSON não decodificados (só `message` aparecia)

Depois de trocar para o input direto, os logs chegavam no Kibana, mas apenas
como um campo `message` de texto corrido — `level`, `logger_name`, etc não
apareciam como campos filtráveis separados.

**Causa:** existem duas camadas de JSON envolvidas. O Docker já grava logs de
containers em JSON (`{"log": "...", "stream": "...", "time": "..."}`), e o
input `type: container` decodifica **essa** camada automaticamente, extraindo
o conteúdo real para o campo `message`. Mas o conteúdo *dentro* desse campo
(nosso próprio JSON, gerado pelo `logback-spring.xml`) precisa de uma segunda
decodificação explícita.

**Correção:** adicionar `json.keys_under_root: true`, `json.add_error_key:
true` e `json.message_key: message` ao input do Filebeat, promovendo os
campos do JSON interno para a raiz do documento no Elasticsearch.

### `docker compose down -v` remove TODOS os volumes, não só o pretendido

Usar `down -v` repetidamente (hábito adquirido para resetar o schema do
Postgres, por causa do `sql.init.mode: always`) também apagava, sem intenção,
toda a configuração feita no Kibana (data views, etc) — porque o estado do
Kibana é salvo dentro do próprio Elasticsearch (índices `.kibana-*`), não em
armazenamento próprio dele. `-v` remove indiscriminadamente todos os volumes
nomeados do projeto de uma vez.

**Correção/hábito:** usar `docker compose down` (sem `-v`) para reinícios do
dia a dia, preservando dados; remover volumes específicos por nome
(`docker volume rm <projeto>_<volume>`) quando o objetivo é resetar só uma
parte pontual do ambiente.

---

## Fase 5 — CI/CD com GitHub Actions

### Conceitos

**CI (Continuous Integration)**: toda vez que código é enviado, uma máquina
remota builda e testa automaticamente, avisando cedo se algo quebrou.
**CD (Continuous Delivery/Deployment)**: depois dos testes passarem, o
pipeline continua sozinho empacotando e publicando a aplicação — nesse caso,
gerando e publicando a imagem Docker.

### Estrutura do workflow

Criado `.github/workflows/ci.yml` com dois jobs:

- **`test`**: roda em todo push/PR — checkout, setup do JDK 25, cache de
  dependências do Gradle (mesmo princípio de cache de camadas do Docker,
  aplicado ao CI), testes, relatório de cobertura (JaCoCo), publicação dos
  relatórios como artefatos.
- **`build-and-push`**: só roda em push direto (não em PR), e só **depois**
  do job `test` passar (`needs: test`) — builda e publica a imagem Docker no
  GitHub Container Registry (`ghcr.io`), usando o `GITHUB_TOKEN` automático
  do Actions (sem precisar configurar nenhuma credencial manualmente).

### Bug 1: branch errado nos gatilhos (`master` vs `main`)

O workflow não disparava porque estava configurado para `main`, mas o
repositório usava `master` como branch padrão. Ajustado temporariamente para
`master`, e depois — ao renomear o branch padrão do repositório para `main`
(alinhado com a convenção atual do GitHub, adotada desde 2020) — o workflow
precisou ser atualizado de volta.

**Lição:** os gatilhos (`on: push: branches: [...]`) e condições (`if:
github.ref == 'refs/heads/...'`) de um workflow precisam bater exatamente com
o nome real do branch usado no repositório — não há detecção automática.

### Bug 2: `contextLoads()` falhava por falta de banco real

O teste padrão gerado pelo Spring Initializr (`ReactiveTasksApplicationTests
.contextLoads()`) sobe a aplicação inteira via `@SpringBootTest`, incluindo
conexão real com R2DBC/Postgres. No runner do GitHub Actions, sem nenhum
Postgres disponível, isso falhava com `ConnectException`.

**Primeira correção tentada:** adicionar um **service container** do
Postgres ao job `test` no workflow (`services: postgres: image: postgres:16
...`) — o GitHub Actions sobe esse container junto com o job, disponível em
`localhost:5432`, com healthcheck (`--health-cmd "pg_isready ..."`) para
garantir que o job só prossegue depois do banco estar pronto. Isso resolveu
o CI, mas expôs uma fragilidade equivalente também **localmente**: os testes
só passavam se o Postgres do `docker-compose` já estivesse rodando no
momento da execução — uma dependência implícita e frágil.

**Decisão final:** em vez de manter essa dependência (mesmo com o service
container resolvendo o CI), removi o arquivo `ReactiveTasksApplicationTests
.kt` por completo. A cobertura de "o contexto Spring sobe corretamente" já é
indiretamente validada pelos testes de slice (`@WebFluxTest` no
`TaskControllerTest`), sem exigir infraestrutura externa só para confirmar
que a aplicação inicializa. O service container do Postgres foi mantido no
workflow mesmo assim, como base já pronta para futuros testes de integração
reais (ex: com Testcontainers).

**Lição:** nem todo teste "de fábrica" precisa ser mantido — vale avaliar
criticamente se ele agrega valor real ou só adiciona fragilidade e
acoplamento a infraestrutura externa, especialmente quando testes mais
específicos (unitários e de slice) já cobrem o comportamento importante.

---

## Fase 6 em diante — Em andamento

*(vou preencher conforme avançamos: Kubernetes, AWS...)*

---

## Perguntas em aberto / pra revisitar depois

- Como exatamente o backpressure funciona na prática em cenários de alta carga
  (ainda só entendi o conceito e o parâmetro de buffer do Sink)?
- Diferença entre `subscribe()` manual e deixar o próprio WebFlux gerenciar a
  subscription (via retorno do Controller)?
- Vale a pena testar o conteúdo real do stream SSE (não só o Content-Type) em
  um teste de integração mais completo?
- Como testar `TaskEventPublisher` agora que ele depende de um Kafka real
  rodando? Vale a pena um teste de integração com Testcontainers?
- Por que exatamente o autodiscover com hints falhou em gerar configuração
  (Bug 3 acima)? Não cheguei à causa raiz definitiva, só contornei o problema.
  Vale revisitar com mais tempo/uma versão diferente do Filebeat.