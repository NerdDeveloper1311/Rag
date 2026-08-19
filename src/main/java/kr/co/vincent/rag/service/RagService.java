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
			List<Message> messages = new ArrayList<>();
			if (hasImages) {
				for (MultipartFile file : files) {
					try {
						if (!file.isEmpty()) {
							byte[] bytes = file.getBytes();
							String contentType = file.getContentType();
							var mimeType = (contentType != null) ? MimeTypeUtils.parseMimeType(contentType) : MimeTypeUtils.IMAGE_JPEG;
							Media media = new Media(mimeType, new ByteArrayResource(bytes));
							messages.add( UserMessage.builder().text( "" ).media( media ).build() );
						}
					} catch (IOException e) {
						log.error("파일 데이터 변환 실패", e);
					}
				}
			}

			String visionPromptText = """
			당신은 여러 장의 발 사진을 상호 대조하고 입체적으로 분석하는 전문 족형 분석가입니다.
		    제공된 각 이미지(예: 위에서 본 뷰, 옆에서 본 뷰 등)를 종합적으로 검증하여 하나의 최종 결론을 도출하세요.
		   
		    [분석 가이드라인]
		    1. 족형: 발가락 길이 배열(엄지가 가장 길면 이집트형, 검지가 길면 그리스형, 비슷하면 로마형)을 분석합니다.
		    2. 발볼: 발의 너비와 비례를 분석하여 넓음, 보통, 좁음 중 하나로 판정합니다.
		    3. 발등: 특히 측면이나 대각선 사진을 참고하여 발등의 경사와 높이를 분석(높음, 보통, 낮음)합니다. 시각적 확인이 어려울 경우 다른 사진의 실루엣을 통해 추론하세요.
		
		    반드시 아래 형식으로만 답변해야 하며, 형식을 벗어난 부연 설명이나 마크다운 태그(예: ```)는 절대 포함하지 마세요. 반드시 한국어로 작성해야 합니다.
		
		    [주의] 만약 제공된 사진들이 발 사진이 아니거나, 화질이 너무 낮아 판별이 절대 불가능한 경우, 아래 양식을 무시하고 오직 "REJECT_IMAGE" 라고만 출력하세요.
		
		    ■ 최종 분석 결과
		    1. 종합 발 형태: (이집트형, 로마형, 그리스형 중 택1)
		    2. 종합 발볼 상태: (넓음, 보통, 좁음 중 택1)
		    3. 종합 발등 상태: (높음, 보통, 낮음 중 택1)
            """;

			UserMessage visionUserMessage = UserMessage.builder()
				.text( visionPromptText )
				.build();

			messages.add( visionUserMessage );

			OllamaChatOptions visionOptions = OllamaChatOptions.builder()
				.model("llama3.2-vision:11b")
				.temperature(0.2)
				.topP(0.85)
				.stop( List.of( "<|im_start|>", "<|im_end|>" ) )
				.build();

			Prompt visionPrompt = new Prompt( messages, visionOptions );
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

	public Flux<String> chatWithDocument( String query ) {
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