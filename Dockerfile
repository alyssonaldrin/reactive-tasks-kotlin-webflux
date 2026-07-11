# ===== Etapa 1: Build =====
# Imagem com o JDK completo, usada só para compilar. Não vai para a imagem final.
FROM eclipse-temurin:25-jdk AS build

WORKDIR /app

# Copia primeiro apenas os arquivos de configuração do Gradle (não o código-fonte).
# Isso aproveita o cache de camadas do Docker: se você só mudar uma classe Kotlin,
# o Docker não precisa baixar as dependências de novo, só recompilar.
COPY gradlew .
COPY gradle gradle
COPY build.gradle.kts settings.gradle.kts ./

# Baixa as dependências numa camada separada (também aproveita cache)
RUN chmod +x gradlew && ./gradlew dependencies --no-daemon

# Só agora copia o código-fonte de verdade
COPY src src

# Compila o .jar executável, pulando os testes (testes já rodam no CI, não precisam
# rodar de novo durante o build da imagem — isso será configurado na Fase 5)
RUN ./gradlew bootJar --no-daemon -x test

# ===== Etapa 2: Runtime =====
# Imagem enxuta, só com o JRE (Java Runtime Environment) — não precisa do JDK
# completo (compilador, ferramentas de build) para simplesmente RODAR a aplicação.
FROM eclipse-temurin:25-jre AS runtime

WORKDIR /app

# Cria um usuário não-root para rodar a aplicação (boa prática de segurança:
# se algo comprometer o container, o processo não roda como root)
RUN groupadd -r spring && useradd -r -g spring spring
USER spring

# Copia SÓ o .jar gerado na etapa de build — nada de código-fonte, Gradle,
# cache de dependências ou qualquer coisa além do artefato final
COPY --from=build /app/build/libs/*.jar app.jar

EXPOSE 8080

ENTRYPOINT ["java", "-jar", "app.jar"]