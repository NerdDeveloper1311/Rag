package kr.co.vincent.rag.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.chat.messages.Message;
import org.springframework.ai.chat.messages.SystemMessage;
import org.springframework.ai.chat.messages.UserMessage;
import org.springframework.ai.chat.prompt.Prompt;
import org.springframework.ai.chat.prompt.PromptTemplate;
import org.springframework.ai.chat.prompt.SystemPromptTemplate;
import org.springframework.ai.content.Media;
import org.springframework.ai.document.Document;
import org.springframework.ai.ollama.OllamaChatModel;
import org.springframework.ai.ollama.api.OllamaChatOptions;
import org.springframework.ai.vectorstore.VectorStore;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.stereotype.Service;
import org.springframework.util.MimeTypeUtils;
import org.springframework.web.multipart.MultipartFile;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class RagService {

	private final VectorStore vectorStore;
	private final OllamaChatModel chatModel;

	public Flux<String> processMultimodalStream(List<MultipartFile> files, String query) {
		boolean hasImages = files != null && !files.isEmpty() && !files.get(0).isEmpty();
		boolean hasText = query != null && !query.strip().isEmpty();

		if (hasText && !hasImages) {
			return chatWithDocument(query);
		}

		if (!hasText && !hasImages) {
			return Flux.just("질문 내용이나 이미지를 추가해 주세요.");
		}

		return Mono.fromCallable(() -> {
				List<Media> mediaList = new ArrayList<>();
				if (hasImages) {
					for (MultipartFile file : files) {
						try {
							if (!file.isEmpty()) {
								byte[] bytes = file.getBytes();
								String contentType = file.getContentType();
								var mimeType = (contentType != null) ? MimeTypeUtils.parseMimeType(contentType) : MimeTypeUtils.IMAGE_JPEG;
								mediaList.add(new Media(mimeType, new ByteArrayResource(bytes)));
							}
						} catch (IOException e) {
							log.error("파일 데이터 변환 실패", e);
						}
					}
				}

				String visionPromptText = """
                당신은 제출된 다량의 발 사진들을 종합적으로 대조하고 감정하는 전문 족형 분석가입니다.
                반드시 한국어(Korean)로만 자연스럽게 답변하십시오.
                제공된 이미지 체인에서 발의 실루엣, 발볼 넓이, 발가락 길이 배열을 복합 검증해야 합니다.
                만약 모든 사진이 판별하기 불가능하다면 오직 "REJECT_IMAGE" 라고만 정확히 출력하세요.
                
                양식에 맞춰 도출해내야 하는 최종 신체 스펙:
                1. 종합 발 형태 (이집트형, 로마형, 그리스형 중 하나)
                2. 종합 발볼 상태 (넓음, 보통, 좁음)
                3. 종합 발등 상태 (높음, 보통, 낮음)
            """;

				if (hasText) {
					visionPromptText += "\n[사용자 질문 컨텍스트]\n" + query;
				}

				UserMessage visionUserMessage = UserMessage.builder()
					.text(visionPromptText)
					.media(mediaList.toArray(new Media[0]))
					.build();

				OllamaChatOptions visionOptions = OllamaChatOptions.builder()
					.model("qwen2.5vl:7b")
					.temperature(0.4)
					.topP(0.85)
					.build();

				Prompt visionPrompt = new Prompt(List.of(visionUserMessage), visionOptions);
				return chatModel.call(visionPrompt).getResult().getOutput().getText().trim();
			})
			.subscribeOn(Schedulers.boundedElastic())
			.flatMapMany(footFeatures -> {
				if (footFeatures.contains("REJECT_IMAGE") || footFeatures.length() < 5) {
					return Flux.just("⚠️ **제공해주신 이미지 자료로는 정확한 족형 분석이 불가능합니다.** 다각도에서 다시 촬영해 주세요.");
				}

				String searchKeyword = footFeatures;
				if (hasText) { searchKeyword += " " + query; }

				List<Document> similarShoes = vectorStore.similaritySearch(searchKeyword);
				String context = similarShoes.stream()
					.map(Document::getText)
					.collect(Collectors.joining("\n\n"));

				String systemPromptText = """
	            너는 클라이밍 장비 전문가이자 유저의 통합 발 분석 정보와 질문을 바탕으로 최적의 암벽화를 매칭하는 AI 가이드이다.
	            철저하게 제공된 [검색된 암벽화 데이터]의 스펙 정보에 기반하여 브랜드와 모델명을 틀리지 말고 마크다운 표와 불릿 구조를 충실히 활용하라.
            """;

				String userPromptTemplateText = """
	            # [종합 발 분석 스펙 문서]
	            {footFeatures}
	
	            # [질문 및 요구사항]
	            {userQuery}
	
	            # [검색된 암벽화 데이터베이스]
	            {context}
            """;

				SystemPromptTemplate systemMessageTemplate = new SystemPromptTemplate(systemPromptText);
				Message systemMessage = systemMessageTemplate.createMessage();

				UserMessage userMessage = new UserMessage(
					new PromptTemplate(userPromptTemplateText).render(Map.of(
						"footFeatures", footFeatures,
						"userQuery", hasText ? query : "발에 딱 맞는 최적의 암벽화를 골라주세요.",
						"context", context
					))
				);

				OllamaChatOptions options = OllamaChatOptions.builder()
					.model("gemma4:e4b")
					.temperature(0.5)
					.build();

				return chatModel.stream(new Prompt(List.of(systemMessage, userMessage), options))
					.map(chunk -> {
						if (chunk == null || chunk.getResult() == null || chunk.getResult().getOutput() == null) return "";
						String text = chunk.getResult().getOutput().getText();
						// [수정 핵심] 공백([SPACE])과 더불어 줄바꿈 기호(\n)도 토큰화 처리하여 데이터 무결성 보장
						return text == null ? "" : text.replace(" ", "[SPACE]").replace("\n", "[NEWLINE]");
					});
			});
	}

	public Flux<String> chatWithDocument(String query) {
		return Mono.fromCallable(() -> {
				List<Document> similarDocuments = vectorStore.similaritySearch(query);
				return similarDocuments.stream()
					.map(Document::getText)
					.collect(Collectors.joining("\n\n"));
			})
			.subscribeOn(Schedulers.boundedElastic())
			.flatMapMany(context -> {
				if (context.strip().isEmpty()) {
					return Flux.just("제공된 문서에 해당 질문과 관련된 내용을 찾을 수 없습니다.");
				}

				String systemPromptText = "제공된 [컨텍스트] 정보 내에서 팩트 위주로 대답하세요.\n\n[컨텍스트]\n{context}";
				SystemPromptTemplate template = new SystemPromptTemplate(systemPromptText);
				String formattedSystemPrompt = template.render(Map.of("context", context));

				Prompt prompt = new Prompt(List.of(new SystemMessage(formattedSystemPrompt), new UserMessage(query)));

				return chatModel.stream(prompt)
					.map(chunk -> {
						if (chunk == null || chunk.getResult() == null || chunk.getResult().getOutput() == null) return "";
						String content = chunk.getResult().getOutput().getText();
						// [수정 핵심] 일반 대화 스트림에서도 마크다운 유지 조치
						return content == null ? "" : content.replace(" ", "[SPACE]").replace("\n", "[NEWLINE]");
					});
			});
	}
}