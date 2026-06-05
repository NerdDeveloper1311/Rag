# 🤖👣🧗 참지말고 추천 받자(가제)

---

## 📌 목적
* PDF 등 문서의 내용 chunk화 하여 임베딩 후 Local LLM 에 사용할 수 있도록 적용
* 인체 치수 조사 데이터를 바탕으로 발볼, 뒷꿈치, 발가락 길이 등의 데이터 임베딩 화
* 이미지를 인식하여 데이터화 시킬수 있는 내용을 임베딩 화

## 📌 프로젝트 아키텍처
* Spring Boot 3.5.8
  * web
  * webflux(Reactive Web)
  * thymeleaf(Template Engine)
* Spring AI 1.1.2
  * Ollama(Local LLM)
  * Elasticseach vector store
  * Tika Document Reader(문서 OCR 혹은 Read)
* Compile 용
  * ProjectLombok
* Docker Container 활용
  * Ollama(Local LLM)
  * Open-WebUI(테스트용 LLM 웹)
  * PgVector(Vector 저장용 PostgreSQL)
  * Elasticsearch(Vector 저장용)

## 📌 초기 세팅
### docker-compose.yml
<details>
<summary>코드 접기/펼치기</summary>
   
```yaml
services:
  ollama:
    image: ollama/ollama:latest
    container_name: ollama
    restart: unless-stopped
    ports:
      - "11434:11434"
    shm_size: '16gb'
    environment:
      - OLLAMA_HOST=0.0.0.0
    volumes:
      - /d/DockerVolume/ollama:/root/.ollama
    deploy:
      resources:
        reservations:
          devices:
            - driver: nvidia
              count: all
              capabilities: [gpu]
    networks:
      - spring-ai-net

  open-webui:
    image: ghcr.io/open-webui/open-webui:main
    container_name: open-webui
    restart: unless-stopped
    ports:
      - "3000:8080"
    environment:
      - 'OLLAMA_BASE_URL=http://ollama:11434'
      - 'WEBUI_SECRET_KEY=1q2w3e4r'
    volumes:
      - /d/DockerVolume/open-webui:/app/backend/data
    depends_on:
      - ollama
    networks:
      - spring-ai-net
  
  pgvector:
    image: pgvector/pgvector:pg16
    container_name: pgvector
    restart: unless-stopped
    ports:
      - "5432:5432"
    environment:
      - POSTGRES_DB=vector_db
      - POSTGRES_USER=vincent
      - POSTGRES_PASSWORD=1q2w3e4r
    volumes:
      - /d/DockerVolume/pgvector:/var/lib/postgresql/data
    networks:
      - spring-ai-net

  elasticsearch:
    image: docker.elastic.co/elasticsearch/elasticsearch:8.12.2
    container_name: es-rag
    restart: unless-stopped
    ports:
      - "9200:9200"
    entrypoint: >
      bash -c "
        if ! elasticsearch-plugin list | grep -q 'analysis-nori'; then
          elasticsearch-plugin install --batch analysis-nori;
        fi;
        exec /usr/local/bin/docker-entrypoint.sh elasticsearch
      "
    environment:
      - node.name=es
      - cluster.name=es-rag-cluster
      - discovery.type=single-node
      - ELASTIC_PASSWORD=1q2w3e4r
      - xpack.security.enabled=true
      - xpack.security.http.ssl.enabled=false
      - ES_JAVA_OPTS=-Xms4g -Xmx4g
    ulimits:
      memlock:
        soft: -1
        hard: -1
    volumes:
      - /d/DockerVolume/elasticsearch:/usr/share/elasticsearch/data
    networks:
      - spring-ai-net

networks:
  spring-ai-net:
    driver: bridge
```

</details>

### build.gradle
<details>
<summary>코드 접기/펼치기</summary>

```gradle
plugins {
    id 'java'
    id 'org.springframework.boot' version '3.5.8'
    id 'io.spring.dependency-management' version '1.1.7'
}

ext {
    set('springAiVersion', "1.1.2")
}

group = 'kr.co.vincent'
version = '0.0.1-SNAPSHOT'
description = 'Rag'

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

repositories {
    mavenCentral()
    maven { url 'https://repo.spring.io/milestone' }
}

dependencies {
    implementation 'org.springframework.boot:spring-boot-starter-web'
    implementation 'org.springframework.boot:spring-boot-starter-webflux'
    implementation 'org.springframework.boot:spring-boot-starter-thymeleaf'

    // Spring AI 핵심 및 Ollama, Elasticsearch 지원
    implementation 'org.springframework.ai:spring-ai-starter-model-ollama'
    implementation 'org.springframework.ai:spring-ai-starter-vector-store-elasticsearch'

    // PDF 및 다양한 문서 파싱을 위한 Tika Reader 추가
    implementation 'org.springframework.ai:spring-ai-tika-document-reader'

    compileOnly 'org.projectlombok:lombok'
    annotationProcessor 'org.projectlombok:lombok'
    testImplementation 'org.springframework.boot:spring-boot-starter-test'
    testImplementation 'io.projectreactor:reactor-test'
}

dependencyManagement {
    imports {
        mavenBom "org.springframework.ai:spring-ai-bom:${springAiVersion}"
    }
}

tasks.named('test') {
    useJUnitPlatform()
}
```

</details>

### application.yml
<details>
<summary>코드 접기/펼치기</summary>

```yaml
spring:
    application:
        name: rag
    ai:
        ollama:
            base-url: http://localhost:11434
            chat:
                options:
                    model: gemma4:e4b
                    temperature: 0.7
            embedding:
                options:
                    model: embeddinggemma
        vectorstore:
            elasticsearch:
                initialize-schema: false
                index-name: rag-vector-index
    elasticsearch:
        uris: http://localhost:9200
        username: elastic
        password: 1q2w3e4r
    output:
        ansi:
            enabled: always
    web:
        resources:
            cache:
                cachecontrol:
                    no-cache: true
                    no-store: true
                    must-revalidate: true
    servlet:
        multipart:
            enabled: true
            max-file-size: 30MB
            max-request-size: 30MB
    mvc:
        async:
            request-timeout: -1
logging:
    pattern:
        console: "%clr(%-5level){green} %clr(%logger.%M\\(\\)){cyan}: %msg%n"
```

</details>

## 🧑‍💻 1차 개발 진행
> 일반 텍스트로 데이터를 입력 받아 벡터화 시켜서 Elasticsearch 에 임베딩  
> PDF 등의 문서 파일을 업로드하여 Tika로 읽어들여 벡터화 이후 Elasticsearch에 임베딩  
> 임베딩 된 데이터를 기반으로 질문/답변이 가능하도록 구성  
> 답변은 마크다운 형식으로 표현하도록 구성  
 
## 🧑‍💻 2차 개발 목표
> 인체 치수 조사 데이터를 바탕으로 발볼, 뒷꿈치, 발가락 길이 등의 데이터 임베딩 화  
> 이미지를 인식하여 데이터화 시킬수 있는 내용을 임베딩 화  