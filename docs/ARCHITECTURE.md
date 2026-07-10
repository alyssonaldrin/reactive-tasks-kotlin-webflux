# Reactive Tasks

Projeto de estudo: API reativa de gerenciamento de tarefas, construída para aprender
programação reativa com Spring WebFlux + Kotlin, e depois expandida com CI/CD,
Docker, Kubernetes e AWS.

## Stack

- **Kotlin** + **Spring Boot 4.0** (Java 25)
- **Spring WebFlux** — programação reativa / não bloqueante
- **R2DBC** + **PostgreSQL** — acesso reativo ao banco
- **Docker Compose** — ambiente local
- (em breve) GitHub Actions, Kubernetes, AWS EKS/RDS

## Como rodar localmente

1. Suba o banco:
   \`\`\`bash
   docker compose up -d
   \`\`\`
2. Rode a aplicação pela IDE ou via:
   \`\`\`bash
   ./gradlew bootRun
   \`\`\`
3. A API sobe em `http://localhost:8080`

## Status do projeto

- [x] Setup inicial (Kotlin + WebFlux + R2DBC)
- [ ] CRUD de tarefas
- [ ] Endpoint de ‘streaming’ (SSE)
- [ ] Testes reativos
- [ ] Dockerfile da aplicação
- [ ] Pipeline CI/CD (GitHub Actions)
- [ ] Deploy em Kubernetes local
- [ ] Deploy em AWS (EKS + RDS)

## Documentação adicional

Veja a pasta [`docs/`](./docs) para decisões de arquitetura e diário de aprendizado.