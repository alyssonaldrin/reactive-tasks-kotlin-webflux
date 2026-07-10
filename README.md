# Reactive Tasks

> API reativa de gerenciamento de tarefas construída como projeto de estudo aprofundado em **programação reativa**, *
*Kotlin** e **Spring WebFlux** — evoluindo para um pipeline completo de **CI/CD**, **Docker**, **Kubernetes** e **AWS**.

![Kotlin](https://img.shields.io/badge/Kotlin-2.x-7F52FF?logo=kotlin&logoColor=white)
![Spring Boot](https://img.shields.io/badge/Spring%20Boot-4.0-6DB33F?logo=spring&logoColor=white)
![Java](https://img.shields.io/badge/Java-25-ED8B00?logo=openjdk&logoColor=white)
![WebFlux](https://img.shields.io/badge/Spring-WebFlux-6DB33F?logo=spring&logoColor=white)
![R2DBC](https://img.shields.io/badge/R2DBC-PostgreSQL-4169E1?logo=postgresql&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)
![License](https://img.shields.io/badge/license-MIT-blue)

---

## 📖 Sobre o projeto

Este não é só um CRUD — é um projeto construído deliberadamente para aprender **programação reativa de ponta a ponta**:
desde os fundamentos de `Mono`/`Flux` até um endpoint de streaming em tempo real via Server-Sent Events (SSE), passando
por containerização, pipeline de CI/CD e deploy em nuvem na AWS com Kubernetes.

Cada decisão técnica está documentada em [`docs/ARCHITECTURE.md`](./docs/ARCHITECTURE.md), e o processo de aprendizado (
incluindo dúvidas e "ahá!" moments) está registrado em [`docs/LEARNING-LOG.md`](./docs/LEARNING-LOG.md) — pensado para
ser transparente sobre o processo, não só o resultado.

## ✨ Funcionalidades

- CRUD reativo completo de tarefas (criar, listar, buscar, atualizar status, remover)
- Endpoint de streaming em tempo real via SSE *(em desenvolvimento)*
- Testes reativos com `StepVerifier` e `WebTestClient` *(em desenvolvimento)*
- Pipeline de CI/CD com GitHub Actions *(em desenvolvimento)*
- Deploy containerizado em Kubernetes *(planejado)*
- Infraestrutura na AWS (EKS + RDS) *(planejado)*

## 🛠️ Stack técnica

| Camada          | Tecnologia              | Por quê                                                   |
|-----------------|-------------------------|-----------------------------------------------------------|
| Linguagem       | Kotlin                  | Concisão, null safety, interoperabilidade JVM             |
| Framework       | Spring Boot 4 + WebFlux | Programação reativa não-bloqueante nativa                 |
| Runtime         | Java 25 (LTS)           | Versão LTS mais atual, suportada oficialmente pelo Spring |
| Banco de dados  | PostgreSQL + R2DBC      | Driver relacional totalmente reativo                      |
| Containerização | Docker / Docker Compose | Ambiente reprodutível local                               |
| CI/CD           | GitHub Actions          | Build, testes e publicação de imagem automatizados        |
| Orquestração    | Kubernetes              | Deploy, escalabilidade e resiliência                      |
| Nuvem           | AWS (EKS, RDS, ECR)     | Infraestrutura gerenciada em produção                     |

## 🏗️ Arquitetura

```
Cliente HTTP
     │
     ▼
TaskController  (REST + SSE, camada de entrada)
     │
     ▼
TaskService     (regras de negócio)
     │
     ▼
TaskRepository  (Spring Data R2DBC)
     │
     ▼
PostgreSQL      (via driver reativo)
```

Todo o fluxo, do Controller ao banco, é **não-bloqueante**: nenhuma thread fica parada esperando I/O, permitindo alta
concorrência com poucos recursos.

## 🚀 Como rodar localmente

**Pré-requisitos:** Docker, JDK 25

```bash
# 1. Clone o repositório
git clone https://github.com/alyssonaldrin/reactive-tasks-kotlin-webflux.git
cd reactive-tasks

# 2. Suba o banco de dados
docker compose up -d

# 3. Rode a aplicação
./gradlew bootRun
```

A API sobe em `http://localhost:8080`.

## 🧪 Testando a API

Importe a collection e o ambiente do Postman disponíveis em [`docs/postman/`](./docs/postman):

1. Importe `reactive-tasks.postman_collection.json`
2. Importe `reactive-tasks-local.postman_environment.json`
3. Selecione o ambiente **Local** e execute as requests

| Método   | Endpoint                 | Descrição                     |
|----------|--------------------------|-------------------------------|
| `GET`    | `/api/tasks`             | Lista todas as tarefas        |
| `GET`    | `/api/tasks/{id}`        | Busca uma tarefa por ID       |
| `POST`   | `/api/tasks`             | Cria uma nova tarefa          |
| `PATCH`  | `/api/tasks/{id}/toggle` | Alterna o status de conclusão |
| `DELETE` | `/api/tasks/{id}`        | Remove uma tarefa             |

Documentação detalhada em [`docs/API.md`](./docs/API.md).

## 📌 Status do projeto

- [x] Setup inicial (Kotlin + WebFlux + R2DBC + Java 25)
- [x] CRUD reativo de tarefas
- [x] Documentação da API (Postman)
- [ ] Endpoint de streaming (SSE)
- [ ] Testes reativos (StepVerifier / WebTestClient)
- [ ] Dockerfile da aplicação
- [ ] Pipeline CI/CD (GitHub Actions)
- [ ] Deploy em Kubernetes local
- [ ] Deploy em AWS (EKS + RDS)

## 📚 Documentação adicional

- [`docs/ARCHITECTURE.md`](./docs/ARCHITECTURE.md) — decisões técnicas e alternativas consideradas
- [`docs/LEARNING-LOG.md`](./docs/LEARNING-LOG.md) — diário de aprendizado, conceito por conceito
- [`docs/API.md`](./docs/API.md) — referência completa dos endpoints

## 👤 Autor

**Alysson Aldrin**
Projeto desenvolvido com fins de estudo e portfólio.

## 📄 Licença

Este projeto está sob a licença MIT — veja [`LICENSE`](./LICENSE) para detalhes.