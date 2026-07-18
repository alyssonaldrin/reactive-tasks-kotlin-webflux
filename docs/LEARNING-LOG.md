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

## Desacoplamento: Ports & Adapters + testes com Testcontainers

*(fora da numeração de fases — foi um refinamento pontual, não uma fase nova)*

### `TaskEventPublisher` estava acoplado diretamente ao Kafka

`TaskService` e `TaskController` dependiam da classe concreta
`TaskEventPublisher`, que já nascia 100% implementada com Kafka por dentro.
Isso tornava difícil trocar a tecnologia por trás (ou testar sem Kafka real)
sem alterar várias classes.

**Correção:** separado em interface (`TaskEventPublisher`, o "contrato") e
implementação concreta (`KafkaTaskEventPublisher`, o "adaptador" Kafka),
padrão conhecido como **Ports & Adapters** (Arquitetura Hexagonal).
`TaskService`/`TaskController` passaram a depender só da interface — o
Spring injeta a implementação automaticamente, sem qualquer mudança de código
nessas classes.

### Testes de integração com Testcontainers (respondendo pergunta em aberto)

Escrito `KafkaTaskEventPublisherIntegrationTest`, testando a implementação
Kafka contra um broker **real**, criado sob demanda via Testcontainers —
sem precisar do `docker-compose.yml` rodando manualmente.

**Bug 1 — imagem incompatível:** `apache/kafka:3.9.0` (mesma do
`docker-compose.yml`) falhava com
`advertised.listeners cannot use the nonroutable meta-address 0.0.0.0`.
Causa: mudança recente no Kafka (KIP-853) rejeita `0.0.0.0` como endereço
anunciado no modo KRaft, mas a auto-configuração da classe
`org.testcontainers.kafka.KafkaContainer` ainda usa esse valor por padrão —
descompasso conhecido entre versões. **Correção:** usar
`apache/kafka-native:3.8.0`, versão recomendada oficialmente pela
documentação do módulo Kafka do Testcontainers.

**Bug 2 — race condition ao publicar logo após conectar:** o primeiro
teste falhava de forma intermitente. O `KafkaReceiver` leva um tempo
assíncrono para formar o consumer group e estabelecer a posição "latest";
publicar imediatamente após `stream()` podia fazer a mensagem "ficar para
trás" dessa posição e nunca ser entregue. **Correção:**
`.thenAwait(Duration.ofSeconds(2))` no `StepVerifier`, dando tempo real para
o consumer se estabelecer antes de qualquer publicação. Esse mesmo
comportamento existe na aplicação real, mas é imperceptível na prática — só
fica visível em testes, onde tudo acontece em sequência rápida.

---

## Fase 6 — Kubernetes local

### Conceitos

- **Pod**: menor unidade do Kubernetes, geralmente um container rodando; efêmero (pode morrer e ser recriado a qualquer
  momento).
- **Deployment**: gerencia quantas réplicas de um Pod devem existir e como atualizá-las. Não se cria Pods diretamente —
  cria-se um Deployment, que cuida disso.
- **Service**: dá um endereço de rede estável para um grupo de Pods que vêm e vão, roteando tráfego via **label selector
  ** (o Service escolhe Pods com base em labels, ex: `app: postgres`).
- **ConfigMap**: configuração não-sensível, injetada como variáveis de ambiente.
- **Secret**: igual ao ConfigMap, mas para dados sensíveis. Guardado em Base64 internamente (não é criptografia de
  verdade — nunca versionar Secrets reais em repositório público).

### Ferramentas escolhidas

- **Cluster local**: Kubernetes embutido no Docker Desktop, usando `kind`
  como método de provisionamento por baixo dos panos (opção padrão nas
  versões recentes do Docker Desktop).
- **Interface visual**: **Lens**, conectado ao cluster via o arquivo
  kubeconfig (`~/.kube/config`) — mesmo arquivo que o `kubectl` já usa.

### Manifestos criados (pasta `k8s/`)

- `namespace.yaml`: namespace próprio (`reactive-tasks`), separando os
  recursos do projeto do resto do cluster.
- `postgres-secret.yaml`: credenciais do banco via `Secret`.
- `postgres-deployment.yaml`: `PersistentVolumeClaim` (equivalente ao volume
  nomeado do Docker Compose) + `Deployment` (com `readinessProbe` via
  `pg_isready`, mesmo princípio do `healthcheck` do Compose) + `Service`.
- `app-configmap.yaml`: configuração não-sensível da aplicação (URL do
  banco, usuário).
- `app-deployment.yaml`: `Deployment` com **2 réplicas** desde já (para já
  exercitar múltiplas instâncias, mesmo cenário que motivou a migração para
  Kafka na Fase 4.5) + `readinessProbe`/`livenessProbe` via
  `/actuator/health` (reaproveitando o Actuator da Fase 4.7) + `Service` do
  tipo `LoadBalancer`.

A senha do banco é injetada na aplicação via `secretKeyRef` (lendo do
`Secret` do Postgres), enquanto o resto da configuração vem do `ConfigMap` —
separação proposital entre dado sensível e configuração comum.

### Lens não mostrava Pods mesmo com o cluster certo

Depois de conectar o Lens ao cluster `docker-desktop`, nenhum Pod ou
Deployment aparecia, mesmo com `kubectl get pods -n reactive-tasks`
confirmando que tudo estava `Running`.

**Causa:** o Lens tem um seletor de **namespaces** próprio, independente do
`kubectl`, e por padrão pode estar filtrando só o namespace `default` (ou
nenhum específico). Como criamos tudo no namespace `reactive-tasks`, nada
aparecia até marcar esse namespace explicitamente no filtro do Lens.

**Lição:** ferramentas visuais que consomem a mesma API do `kubectl` podem
ter seus próprios filtros/estado de UI — confirmar sempre via `kubectl`
(fonte da verdade) antes de assumir que algo está errado no cluster em si.

### `kind` não distribui tráfego uniformemente entre réplicas

Com `replicas: 2`, requisições repetidas via `Service` do tipo
`LoadBalancer` sempre bateram no mesmo Pod, mesmo com as duas réplicas
saudáveis (`READY 1/1`, confirmado via `kubectl get endpoints`).

**Causa:** `kind` simula um `LoadBalancer` usando o próprio Docker, sem um
balanceador de carga real por trás — diferente de um ambiente cloud (AWS/EKS)
onde o Load Balancer é um componente de infraestrutura de verdade. Localmente
essa distribuição costuma ser bem menos uniforme, especialmente com poucas
requisições e conexões reaproveitadas (keep-alive).

**Lição:** ambientes Kubernetes locais são ótimos para aprender conceitos e
manifestos, mas alguns comportamentos de infraestrutura (balanceamento de
carga real, storage distribuído, etc) só se manifestam de fato num ambiente
cloud gerenciado. Nem tudo que "parece diferente" localmente é um bug do
manifesto.

---

## Fase 6 (continuação) — Kafka no cluster, HPA e teste de carga

### Kafka em Kubernetes: bind em 0.0.0.0 sem endereço anunciado explícito falha

Configuramos `CONTROLLER://0.0.0.0:9093` como listener, sem uma entrada
correspondente em `KAFKA_ADVERTISED_LISTENERS` (mesmo padrão que funcionava
no `docker-compose.yml`, onde o CONTROLLER nunca usava `0.0.0.0`, sempre um
hostname real). No Kubernetes, isso causava
`advertised.listeners cannot use the nonroutable meta-address 0.0.0.0` —
o Kafka usa o próprio endereço de escuta como fallback para o anunciado
quando não há um explícito, e a versão 3.9 (KIP-853) passou a rejeitar esse
fallback quando ele resulta em `0.0.0.0`.

**Correção parcial 1:** usar a **Downward API** (`fieldRef: status.podIP`)
para obter o IP real do Pod, usando-o como bind tanto do listener de cliente
quanto do controller — evitando `0.0.0.0`. O endereço *anunciado* pra
aplicação continua sendo o nome do Service (`kafka`), estável entre
reinícios.

**Novo problema:** a `readinessProbe` (via `exec`) usava `localhost:9092`,
mas o bind não incluía mais `localhost` (só o IP real do Pod), então a probe
sempre falhava mesmo com o Kafka saudável.

**Tentativa de correção com `$(POD_IP)` na probe:** não funcionou — a
substituição de `$(VAR)` do Kubernetes funciona dentro do bloco `env:`
(confirmado no `KafkaConfig` resolvido, com IPs reais), mas **não** é
aplicada da mesma forma dentro de `readinessProbe.exec.command` — o valor
chegava vazio.

**Correção final:** trocar a probe de `exec` para `tcpSocket: port: 9092` —
mais simples (só testa se a porta aceita conexão, sem precisar resolver
nenhum endereço) e elimina essa classe inteira de problema.

**Lição:** no Docker Compose, o nome do container é resolvido para o IP do
próprio container, permitindo "escutar" nele diretamente. No Kubernetes, o
nome do Service é um endereço virtual (roteado externamente ao Pod) — não dá
para "escutar" nele de dentro do próprio Pod. A Downward API resolve a
necessidade de obter o IP real do Pod, mas essa substituição não é uniforme
em todos os contextos (funciona em `env`, não em `exec.command` de probes).

### `enableServiceLinks: false` — variáveis de ambiente injetadas pelo Service podem colidir

Antes de descobrir a causa real (acima), suspeitei que variáveis de ambiente
que o Kubernetes injeta automaticamente para cada Service do namespace
(compatibilidade com Docker links, ex: `KAFKA_PORT`, `KAFKA_PORT_9092_TCP`)
pudessem interferir na leitura de variáveis `KAFKA_*` feita pela imagem
`apache/kafka`. Adicionei `enableServiceLinks: false` ao Pod como precaução
— não era a causa raiz desse bug específico, mas é uma boa prática geral:
evita colisões de nomes em imagens que fazem varredura ampla de variáveis de
ambiente com um prefixo comum.

### HPA + Metrics Server

Instalado o **Metrics Server** (não vem por padrão no Kubernetes do Docker
Desktop), necessário para o HPA saber o uso de CPU/memória dos Pods.
Precisou da flag `--kubelet-insecure-tls` (via patch JSON, não inline —
aspas simples em JSON inline quebravam no PowerShell), já que certificados
TLS entre nodes não são válidos em cluster local.

Configurado um `HorizontalPodAutoscaler` visando 50% de utilização média de
CPU (relativo ao `requests.cpu` do Deployment), min 2 / max 6 réplicas, com
`behavior` customizado para escalar rápido (útil para observar o teste de
carga) e desescalar com mais cautela (evita "flapping").

### Teste de carga com k6

Script `load-test.js` com estágios de carga crescente/decrescente (`stages`),
misturando 80% leitura (`GET /api/tasks`) e 20% escrita (`POST /api/tasks`,
exercitando o pipeline completo incluindo Kafka). Rodado via Docker
(`grafana/k6`), já que `--network host` não funciona de forma confiável no
Docker Desktop para Windows — resolvido usando `host.docker.internal` como
endereço da aplicação, com o BASE_URL parametrizado via variável de ambiente
do k6 (`__ENV.BASE_URL`).

---

## Fase 6 (continuação) — Observabilidade dentro do cluster (Helm)

Decidido usar **Helm** em vez de manifestos manuais para Prometheus, Grafana
e (planejado) ELK — depois da experiência de configurar o Kafka manualmente
em YAML puro, ficou claro que ferramentas complexas como essas têm
particularidades demais para valer a pena escrever do zero, quando já
existem *charts* maduros e testados pela comunidade.

Instalado `kube-prometheus-stack` (Prometheus + Grafana + kube-state-metrics

+ node-exporter, Alertmanager desabilitado) num namespace próprio
  (`monitoring`), com valores customizados reduzindo requests/limits de
  recursos e desabilitando persistência (ambiente de estudo, dados efêmeros
  são aceitáveis).

### ServiceMonitor: selector busca labels do Service, não do spec.selector

Configurado um `ServiceMonitor` para o Prometheus (gerenciado pelo Operator
do chart) coletar métricas da aplicação. Mesmo com o `ServiceMonitor`
reconhecido (aparecia como scrape pool no Prometheus), nenhum alvo era
encontrado ("0/0 up, No active targets").

**Causa:** o `selector.matchLabels` de um `ServiceMonitor` procura labels no
**próprio objeto Service** (`metadata.labels`), não no `spec.selector` do
Service (usado para rotear tráfego aos Pods) nem nos labels dos Pods
diretamente. Nosso Service não tinha `metadata.labels` — só `spec.selector`.

**Correção:** adicionar `metadata.labels: app: reactive-tasks-app` ao
próprio Service.

**Lição:** `spec.selector` (Service → Pods) e `metadata.labels` (identidade
do próprio objeto) são conceitos relacionados mas distintos no Kubernetes —
um ponto de confusão comum ao configurar observabilidade via Prometheus
Operator.

### Grafana subdimensionado: "context deadline exceeded" generalizado

Depois de recriar o cluster, o Pod do Grafana ficava preso em
`Readiness probe failed: connection refused`, e os logs mostravam
requisições internas (SQLite, indexação de busca) levando 1-3 **minutos**
para completar, muito acima do normal.

**Causa:** os `resources.requests/limits` definidos inicialmente
(`100m`/`128Mi`) eram insuficientes para as versões atuais do Grafana, que
ficou consideravelmente mais pesado (inclui um "mini apiserver" interno,
indexação via bleve, etc). Combinado com o cluster já rodando bastante coisa
(Postgres, Kafka, múltiplas réplicas da app, Prometheus, node-exporter),
havia contenção real de recursos no node.

**Correção:** aumentar os recursos do Grafana via `helm upgrade` (para
`250m`/`384Mi` de request), e considerar aumentar a alocação de CPU/memória
do próprio Docker Desktop caso o node inteiro esteja saturado
(`kubectl top nodes` ajuda a diagnosticar isso).

### Dashboard importado (ID 4701) vazio: uma cadeia de pequenos problemas

Importar um dashboard pronto da comunidade para métricas de JVM/Spring Boot
não "simplesmente funcionou" — precisou de uma sequência de ajustes:

1. **Tag `application` ausente**: o dashboard espera que cada métrica tenha
   uma label `application` com o nome da app — não é algo automático do
   Micrometer, precisa ser configurado explicitamente via
   `management.metrics.tags.application` (setado como variável de ambiente
   `MANAGEMENT_METRICS_TAGS_APPLICATION` no ConfigMap, sem precisar
   rebuildar a imagem, graças ao *relaxed binding* do Spring Boot).
2. **Variável `instance` como multi-value**: com 2 réplicas da aplicação, a
   variável de template `instance` vinha configurada para múltipla seleção
   por padrão. Isso fazia o Grafana substituir `$instance` num formato de
   regex (`(ip1|ip2)`) dentro de uma query que usava igualdade exata (`=`),
   nunca batendo com nenhum valor real. Corrigido desligando
   "Multi-value" e "Include All value" na configuração da variável.
3. **Datasource `${DS_PROMETHEUS}` não resolvida**: alguns painéis
   específicos do dashboard importado mantiveram uma variável de "input" de
   importação (`${DS_PROMETHEUS}`) não substituída pela fonte de dados real,
   mesmo após a importação — corrigido editando o JSON Model do dashboard
   diretamente, substituindo as ocorrências pelo UID real (`prometheus`).

**Lição geral:** dashboards prontos de terceiros economizam trabalho, mas
carregam suposições implícitas (nomes de labels, tags customizadas, convenção
de variáveis) que nem sempre batem de primeira com o setup de quem importa.
Vale sempre confirmar a métrica crua primeiro (via Explore, sem dashboard),
antes de assumir que o problema está na coleta em vez da apresentação —
nesse caso, os dados sempre estiveram lá, corretos, o tempo todo.

---

*(vou preencher conforme avançamos: Kafka no cluster, HPA, teste de carga, AWS...)*

---

## Perguntas em aberto / pra revisitar depois

- Como exatamente o backpressure funciona na prática em cenários de alta carga
  (ainda só entendi o conceito e o parâmetro de buffer do Sink)?
- Diferença entre `subscribe()` manual e deixar o próprio WebFlux gerenciar a
  subscription (via retorno do Controller)?
- Vale a pena testar o conteúdo real do stream SSE (não só o Content-Type) em
  um teste de integração mais completo?