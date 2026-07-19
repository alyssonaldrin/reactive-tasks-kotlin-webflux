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
- O ciclo de vida dos publishers reativos (quando eles começam, terminam, ou
  se resetam) é tão importante quanto a lógica de transformação dos dados —
  um `Sink` não é só uma fila, tem estados (ativo, completo, com erro) que
  afetam todo mundo conectado nele.

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
em si seria mais teste de integração, e o `Flux` de SSE nunca termina sozinho.

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
- "localhost" dentro de um container não é a máquina host nem outros
  containers — é o próprio container. Comunicação entre containers precisa
  passar pela rede que os orquestra (Compose, e depois, Kubernetes) — daí a
  aplicação usar o **nome do serviço** (`postgres`) como hostname, em vez de
  `localhost`.
- `healthcheck` (`depends_on: condition: service_healthy`) evita que a
  aplicação tente conectar no banco antes dele estar realmente pronto para
  aceitar conexões, mesmo já "rodando".

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
- Configurações customizadas (que não vêm de um starter oficial do Spring)
  merecem prefixo próprio no `application.yaml` (`app.kafka.*`), deixando
  claro que não são propriedades interpretadas pela autoconfiguração do
  Spring Boot.
- Nomes de tópicos como **configuração**, não constante fixa no código —
  permite usar tópicos diferentes por ambiente (dev, staging, prod) sem
  recompilar a aplicação.

---

## Fase 4.7 — Observabilidade (métricas com Prometheus + Grafana)

Antes de configurar qualquer ferramenta, revisei os três pilares da
observabilidade: **logs** (o que aconteceu, evento a evento), **métricas**
(números agregados ao longo do tempo — taxa de requisições, latência, uso de
memória) e **traces** (o caminho de uma requisição específica por múltiplos
serviços — não explorado ainda).

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
da aplicação (`/actuator/prometheus`, a cada `scrape_interval`) e coleta os
dados de lá — chamado modelo *pull-based*.

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
Prometheus já coletou e armazenou.

### "Cold start" na primeira requisição não é cache

A primeira requisição a um endpoint (ex: `GET /api/tasks`) demora
visivelmente mais que as seguintes, mesmo sem nenhum cache configurado no
projeto. Causa: fenômeno de "cold start" da JVM — JIT compilation ainda não
otimizou os métodos mais chamados, o pool de conexões R2DBC abre a primeira
conexão real sob demanda, e algumas classes são carregadas/inicializadas só
no primeiro uso.

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

Os logs do Docker já vêm em JSON por padrão (`{"log": "...", "stream": "...",
"time": "..."}`), com o conteúdo real da aplicação "dentro" do campo `log` —
existem, portanto, duas camadas de JSON envolvidas quando a própria aplicação
também loga em JSON estruturado, e cada camada precisa da sua própria
decodificação explícita no Filebeat.

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

Os gatilhos (`on: push: branches: [...]`) e condições
(`if: github.ref == 'refs/heads/...'`) de um workflow precisam bater
exatamente com o nome real do branch usado no repositório — não há detecção
automática.

### Testes que exigem infraestrutura externa são frágeis

O teste padrão gerado pelo Spring Initializr (`contextLoads()`) sobe a
aplicação inteira via `@SpringBootTest`, incluindo conexão real com
R2DBC/Postgres — na prática, um teste de integração disfarçado de teste
unitário, que só passa se houver um Postgres disponível (seja localmente via
Docker Compose, seja via *service container* no CI). Removido em favor da
cobertura já garantida pelos testes de slice (`@WebFluxTest`), que não
dependem de infraestrutura externa para validar que a aplicação está
corretamente configurada. Nem todo teste "de fábrica" precisa ser mantido —
vale avaliar criticamente se ele agrega valor real ou só adiciona
fragilidade e acoplamento a infraestrutura externa.

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

### Testes de integração com Testcontainers

Escrito `KafkaTaskEventPublisherIntegrationTest`, testando a implementação
Kafka contra um broker **real**, criado sob demanda via Testcontainers —
sem precisar do `docker-compose.yml` rodando manualmente. Aprendido que
consumers Kafka levam um tempo assíncrono real para formar o consumer group
e estabelecer sua posição de leitura — em testes, onde publish/subscribe
acontecem em sequência rápida, vale dar uma margem de tempo real
(`.thenAwait(...)`) antes de publicar, para não competir com esse processo
de formação do grupo.

---

## Fase 6 — Kubernetes local

### Conceitos

- **Pod**: menor unidade do Kubernetes, geralmente um container rodando; efêmero (pode morrer e ser recriado a qualquer
  momento).
- **Deployment**: gerencia quantas réplicas de um Pod devem existir e como atualizá-las. Não se cria Pods diretamente —
  cria-se um Deployment, que cuida disso.
- **Service**: dá um endereço de rede estável para um grupo de Pods que vêm e vão, roteando tráfego via **label selector
  ** (o Service escolhe Pods com base em labels, ex: `app: postgres`). Importante: o *seletor* do Service aponta para
  labels dos **Pods**; os *labels do próprio Service* são um conceito separado, usado por outras ferramentas (como o
  `ServiceMonitor` do Prometheus Operator) para encontrar o Service em si.
- **ConfigMap**: configuração não-sensível, injetada como variáveis de ambiente.
- **Secret**: igual ao ConfigMap, mas para dados sensíveis. Guardado em Base64 internamente (não é criptografia de
  verdade — nunca versionar Secrets reais em repositório público).
- **HPA (HorizontalPodAutoscaler)**: escala o número de réplicas de um Deployment automaticamente, com base em métricas
  de uso (CPU, memória) relativas aos `resources.requests` configurados. Depende do **Metrics Server** para saber o
  consumo real de cada Pod.

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
  banco, usuário, tópico/bootstrap do Kafka).
- `app-deployment.yaml`: `Deployment` com **2 réplicas** desde já (para já
  exercitar múltiplas instâncias, mesmo cenário que motivou a migração para
  Kafka na Fase 4.5) + `readinessProbe`/`livenessProbe` via
  `/actuator/health` + `Service` do tipo `LoadBalancer`.
- `kafka-deployment.yaml`: Kafka em modo KRaft rodando como `Deployment`,
  usando a **Downward API** (`fieldRef: status.podIP`) para obter o IP real
  do próprio Pod — diferente do Docker Compose, onde o nome de um container
  é resolvido diretamente para seu próprio IP, permitindo "escutar" nesse
  nome; no Kubernetes, o nome de um Service é um endereço *virtual*, roteado
  de fora do Pod, então não é possível vincular um listener diretamente a
  ele.
- `app-hpa.yaml`: `HorizontalPodAutoscaler` visando 50% de utilização média
  de CPU, min 2 / max 6 réplicas, com `behavior` customizado para escalar
  rápido e desescalar com mais cautela (evita "flapping").

A senha do banco é injetada na aplicação via `secretKeyRef` (lendo do
`Secret` do Postgres), enquanto o resto da configuração vem do `ConfigMap` —
separação proposital entre dado sensível e configuração comum.

### `kind` não distribui tráfego uniformemente entre réplicas

`kind` simula um `LoadBalancer` usando o próprio Docker, sem um balanceador
de carga real por trás — diferente de um ambiente cloud (AWS/EKS) onde o
Load Balancer é um componente de infraestrutura de verdade. Ambientes
Kubernetes locais são ótimos para aprender conceitos e manifestos, mas
alguns comportamentos de infraestrutura (balanceamento de carga real,
storage distribuído, etc) só se manifestam de fato num ambiente cloud
gerenciado.

### Teste de carga com k6

Script `load-test.js` com estágios de carga crescente/decrescente (`stages`),
misturando 80% leitura (`GET /api/tasks`) e 20% escrita (`POST /api/tasks`,
exercitando o pipeline completo incluindo Kafka), usado para validar o HPA
escalando de verdade sob carga real.

### Observabilidade dentro do cluster via Helm

Depois da experiência de configurar o Kafka manualmente em YAML puro, optei
por usar **Helm** (gerenciador de pacotes do Kubernetes) para Prometheus,
Grafana e ELK, em vez de escrever manifestos do zero — ferramentas complexas
como essas têm particularidades demais para valer a pena reinventar, quando
já existem *charts* maduros e testados pela comunidade.

Instalado `kube-prometheus-stack` (Prometheus + Grafana + kube-state-metrics

+ node-exporter) num namespace próprio (`monitoring`) — boa prática comum:
  infraestrutura de observabilidade geralmente vive separada dos namespaces de
  aplicação, compartilhada entre vários projetos num cluster real.

Conectar o Prometheus (gerenciado por um Operator) à aplicação usa um
recurso declarativo chamado `ServiceMonitor`, que substitui a edição manual
de um `prometheus.yml` estático — o Operator reconfigura o Prometheus
automaticamente ao detectar um `ServiceMonitor` no cluster.

Dashboards prontos de terceiros (importados via ID) economizam trabalho, mas
carregam suposições implícitas (nomes de labels, tags customizadas,
convenção de variáveis) que nem sempre batem de primeira com o setup de
quem importa — vale sempre confirmar a métrica crua primeiro (via Explore,
sem dashboard), antes de assumir que o problema está na coleta em vez da
apresentação.

---

## Fase 7 em diante — Em andamento

*(vou preencher conforme avançamos: ELK no cluster, AWS...)*

---

## Perguntas em aberto / pra revisitar depois

- Como exatamente o backpressure funciona na prática em cenários de alta carga
  (ainda só entendi o conceito e o parâmetro de buffer do Sink)?
- Diferença entre `subscribe()` manual e deixar o próprio WebFlux gerenciar a
  subscription (via retorno do Controller)?
- Vale a pena testar o conteúdo real do stream SSE (não só o Content-Type) em
  um teste de integração mais completo?