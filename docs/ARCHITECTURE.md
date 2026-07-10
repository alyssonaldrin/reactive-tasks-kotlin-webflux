# Arquitetura

Este documento registra as principais decisões técnicas do projeto e o raciocínio
por trás delas — não só o "o quê", mas o "por quê".

## Visão geral

```
Cliente HTTP
     │
     ▼
TaskController        (REST + SSE — camada de entrada)
     │           │
     ▼           ▼
TaskService   TaskEventPublisher
     │              (Sink — distribui eventos para streams ativos)
     ▼
TaskRepository        (Spring Data R2DBC)
     │
     ▼
PostgreSQL             (via driver reativo)
```

Todo o fluxo — do Controller ao banco — é **não-bloqueante**. Nenhuma thread fica
parada esperando resposta de I/O (rede, disco, banco); em vez disso, o pipeline
reage a cada dado conforme ele chega.

## Decisões técnicas

### Por que Spring WebFlux em vez de Spring MVC (tradicional)?

O objetivo principal do projeto é justamente aprender o modelo reativo. WebFlux
usa um número pequeno e fixo de threads (event loop, via Netty) para atender
muitas requisições concorrentes, em vez do modelo "uma thread por requisição" do
Spring MVC. Isso importa em cenários de alta concorrência com I/O intenso — não
necessariamente para todo tipo de aplicação, mas é o cenário que este projeto
existe para explorar.

### Por que R2DBC em vez de JDBC?

JDBC é bloqueante por natureza — cada chamada ao banco prende uma thread até a
resposta voltar. Usar WebFlux (não-bloqueante) na camada web e JDBC (bloqueante)
na camada de dados anularia o ganho do modelo reativo, criando um gargalo
escondido. R2DBC mantém o pipeline não-bloqueante de ponta a ponta.

**Trade-off aceito:** o ecossistema R2DBC é menos maduro que o do JDBC (menos
ferramentas, menos exemplos, comunidade menor). Para este projeto de estudo,
esse custo vale a pena pelo aprendizado; em um cenário real, essa maturidade
entraria na decisão.

### Por que Server-Sent Events (SSE) em vez de WebSocket para o streaming?

O caso de uso é unidirecional: o servidor notifica o cliente sobre mudanças em
tarefas, mas o cliente não precisa enviar mensagens de volta pelo mesmo canal.
SSE é mais simples de implementar e consumir para esse cenário (funciona sobre
HTTP comum, reconecta automaticamente no navegador) e é a integração mais
natural com um `Flux` no Spring WebFlux. WebSocket seria a escolha certa se
houvesse comunicação bidirecional (ex: um chat).

### Como o streaming em tempo real funciona (`TaskEventPublisher`)

Usamos `Sinks.many().multicast().onBackpressureBuffer<Task>(256, false)`:

- **`multicast`** — múltiplos clientes podem se conectar ao mesmo stream
  simultaneamente e todos recebem os mesmos eventos.
- **`onBackpressureBuffer(256, ...)`** — se um cliente consumir mais devagar do
  que os eventos chegam, até 256 eventos ficam em buffer antes de qualquer
  descarte, em vez de travar o publisher ou perder dados silenciosamente.
- **`autoCancel = false`** (segundo parâmetro) — por padrão, o Sink se encerra
  permanentemente assim que o último assinante se desconecta, rejeitando
  qualquer conexão futura. Desativar isso mantém o Sink vivo durante todo o
  ciclo de vida da aplicação, aceitando novas conexões a qualquer momento.
  *(Detalhes do debugging desse comportamento em [`LEARNING-LOG.md`](./LEARNING-LOG.md).)*

`TaskService` publica um evento (via `doOnNext`, um efeito colateral que não
altera o dado) toda vez que uma tarefa é criada ou atualizada, sem acoplar a
lógica de negócio à lógica de streaming.

### Por que `sql.init.mode: always` em vez de uma ferramenta de migração?

Nesta fase do projeto (aprendizado, ambiente local), recriar o schema a cada
subida da aplicação simplifica a iteração. Isso **não é adequado para produção**:
a evolução planejada do projeto inclui adotar Flyway (ou equivalente) antes do
deploy em ambientes reais, para versionar alterações de schema de forma segura
e incremental.

## Decisões em aberto / a revisitar

- **Tratamento de erros e status HTTP**: hoje os endpoints não têm tratamento
  explícito de erros (ex: buscar uma tarefa inexistente ainda não retorna um
  404 claro). Isso será endereçado ao lado dos testes na Fase 3.
- **Validação de entrada**: `CreateTaskRequest` ainda não usa Bean Validation
  (`@Valid`, `@NotBlank`), apesar da dependência já estar no projeto.
- **Migração de schema**: ver ponto acima sobre Flyway.

Essas lacunas são conhecidas e intencionais nesta fase — o projeto está sendo
construído incrementalmente, seguindo o roteiro documentado no `README.md`.