# Referência da API

Base URL local: `http://localhost:8080`

Todos os endpoints (exceto o de streaming) trocam dados em `application/json`.

---

## `GET /api/tasks`

Lista todas as tarefas cadastradas.

**Resposta `200 OK`**

```json
[
  {
    "id": 1,
    "title": "Aprender WebFlux",
    "completed": false,
    "createdAt": "2026-07-10T14:32:00"
  },
  {
    "id": 2,
    "title": "Configurar CI/CD",
    "completed": false,
    "createdAt": "2026-07-10T15:01:00"
  }
]
```

---

## `GET /api/tasks/{id}`

Busca uma tarefa específica pelo ID.

**Parâmetros de URL**

| Nome | Tipo   | Descrição    |
|------|--------|--------------|
| `id` | `Long` | ID da tarefa |

**Resposta `200 OK`**

```json
{
  "id": 1,
  "title": "Aprender WebFlux",
  "completed": false,
  "createdAt": "2026-07-10T14:32:00"
}
```

> ⚠️ Tratamento de erro para ID inexistente ainda não implementado — será
> endereçado na Fase 3 (testes), junto com códigos de status apropriados
> (`404 Not Found`).

---

## `POST /api/tasks`

Cria uma nova tarefa.

**Corpo da requisição**

```json
{
  "title": "Estudar Kubernetes"
}
```

| Campo   | Tipo     | Obrigatório | Descrição        |
|---------|----------|-------------|------------------|
| `title` | `String` | Sim         | Título da tarefa |

**Resposta `200 OK`**

```json
{
  "id": 3,
  "title": "Estudar Kubernetes",
  "completed": false,
  "createdAt": "2026-07-10T16:10:00"
}
```

**Efeito colateral:** publica um evento no stream de tarefas (ver
[`GET /api/tasks/stream`](#get-apitasksstream)).

---

## `PATCH /api/tasks/{id}/toggle`

Alterna o status `completed` da tarefa (`true` ↔ `false`). Não requer corpo na
requisição.

**Parâmetros de URL**

| Nome | Tipo   | Descrição    |
|------|--------|--------------|
| `id` | `Long` | ID da tarefa |

**Resposta `200 OK`**

```json
{
  "id": 1,
  "title": "Aprender WebFlux",
  "completed": true,
  "createdAt": "2026-07-10T14:32:00"
}
```

**Efeito colateral:** publica um evento no stream de tarefas.

---

## `DELETE /api/tasks/{id}`

Remove uma tarefa pelo ID.

**Parâmetros de URL**

| Nome | Tipo   | Descrição    |
|------|--------|--------------|
| `id` | `Long` | ID da tarefa |

**Resposta `200 OK`** — corpo vazio.

---

## `GET /api/tasks/stream`

Abre uma conexão persistente via **Server-Sent Events (SSE)**. O servidor envia
um evento toda vez que uma tarefa é criada ou tem seu status alterado — sem
necessidade de o cliente ficar consultando (*polling*).

**Header de resposta:** `Content-Type: text/event-stream`

**Formato de cada evento**

```
data: {"id":3,"title":"Estudar Kubernetes","completed":false,"createdAt":"2026-07-10T16:10:00"}
```

**Como testar:**

- Via navegador: acesse a URL diretamente; a aba permanece "carregando"
  enquanto a conexão está aberta.
- Via terminal (Windows/PowerShell, forçando o curl real):
  ```bash
  curl.exe -N -H "Accept: text/event-stream" http://localhost:8080/api/tasks/stream
  ```
- Via Postman: crie/abra a request na collection (`docs/postman/`), pasta
  **"Streaming (SSE)"`, e clique em **Send** — o Postman (v10.10+) reconhece o
  `Content-Type` automaticamente e exibe os eventos chegando em tempo real.

A conexão permanece aberta indefinidamente até o cliente encerrá-la.

---

## Coleção Postman

Uma collection completa, com todos os endpoints acima já configurados e um
ambiente local pronto para uso, está disponível em
[`docs/postman/`](./postman). Veja instruções de importação no
[`README.md`](../README.md#-testando-a-api).