package kr.co.vincent.rag.service;

import kr.co.vincent.rag.dto.ShoeRequest;
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
import org.springframework.ai.reader.tika.TikaDocumentReader;
import org.springframework.ai.transformer.splitter.TokenTextSplitter;
import org.springframework.ai.vectorstore.VectorStore;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Service;
import org.springframework.util.MimeTypeUtils;
import org.springframework.web.multipart.MultipartFile;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class RagService {

	private final VectorStore vectorStore;
	private final OllamaChatModel chatModel;

	public Flux<String> chatWithDocument( String query ) {
		return Mono.fromCallable( () -> {
			List<Document> similarDocuments = vectorStore.similaritySearch( query );
			return similarDocuments.stream()
				.map( Document::getText )
				.collect( Collectors.joining( "\n\n" ) );
		} )
		.subscribeOn( Schedulers.boundedElastic() )
		.flatMapMany( context -> {
			if ( context.strip().isEmpty() ) {
				return Flux.just( "제공된[SPACE]문서에[SPACE]해당[SPACE]질문과[SPACE]관련된[SPACE]내용을[SPACE]찾을[SPACE]수[SPACE]없습니다." );
			}

			String systemPromptText = """
				당신은 사용자가 연동한 커스텀 지식 문서 및 주입된 암벽화 스펙 데이터베이스를 기반으로 답변하는 유능한 AI 어시스턴트입니다.
				반드시 제공된 [컨텍스트] 정보(일반 문서 데이터 및 암벽화 관련 명세 지식 포함)를 최우선 바탕으로 하여 질문에 정확하게 답변해야 합니다.
				
				[지침]
				1. 제공된 [컨텍스트]에 기술되어 있는 정보와 스펙 매칭 지식을 유기적으로 연결하여 답변을 구성하세요.
				2. 컨텍스트만으로 질문에 대한 답을 명확하게 도출할 수 없다면, 억지로 허위 사실을 지어내지 말고 "제공된 정보 내에서 관련 답변을 찾을 수 없습니다." 라고 안내하십시오.
				3. 답변할 때는 문맥상 흐름이 자연스럽도록 결론 및 명확한 팩트 위주로 대답하세요.
				4. elasticsearch vector data의 metadata.type의 "qna_evaluation"에서 metadata.create_at 날짜가 최근 3개월 이내의 metadata.score가 긍정적인 답변들을 참고해서 대답하세요.
				
				[컨텍스트]
				{context}
			""";

			SystemPromptTemplate template = new SystemPromptTemplate( systemPromptText );
			String formattedSystemPrompt = template.render( Map.of( "context", context ) );
			SystemMessage systemMessage = new SystemMessage( formattedSystemPrompt );
			UserMessage userMessage = new UserMessage( query );

			Prompt prompt = new Prompt( List.of( systemMessage, userMessage ) );

			return chatModel.stream( prompt )
				.map( chunk -> {
					if ( chunk == null || chunk.getResult() == null || chunk.getResult().getOutput() == null ) {
						return "";
					}
					String content = chunk.getResult().getOutput().getText();
					if ( content == null ) return "";

					return content.replace( " ", "[SPACE]" );
				} );
		} );
	}

	public Flux<String> recommendShoesByFootImage( MultipartFile file ) {
		return Mono.fromCallable( () -> {
			byte[] bytes = file.getBytes();

			// 1. 프롬프트 구조화 및 가이드라인 출력 조건 명시
			String promptText = """
                당신은 발 이미지를 판별하는 전문 분석가입니다. 
                반드시 한국어(Korean)로만 자연스럽게 답변하십시오.
                
                [분석 가능 여부 체크]
                제공된 이미지에서 발의 형태, 발볼, 발등을 명확히 식별할 수 있는지 확인하세요.
                만약 사진이 심하게 흔들렸거나, 어둡거나, 구도가 불량하거나, 발 전체가 보이지 않는 등 분석이 불가능하다면 
                다른 설명 없이 정확히 오직 "REJECT_IMAGE" 라고만 답변하십시오.
                
                [분석 지시사항]
                이미지 분석이 가능하다면, 아래 3가지 요소를 명확히 도출해 주세요.
                1. 발 형태 (이집트형, 로마형, 그리스형 중 해당 항목)
                2. 발볼의 상대적인 넓이 (넓음, 보통, 좁음)
                3. 발등의 높낮이 (높음, 보통, 낮음)
            """;

			UserMessage userMessage = UserMessage.builder()
				.text( promptText )
				.media( new Media( MimeTypeUtils.IMAGE_JPEG, new ByteArrayResource( bytes ) ) )
				.build();

			OllamaChatOptions options = OllamaChatOptions.builder()
				.model( "qwen2.5vl:7b" )
				.temperature( 0.5 )
				.topP( 0.85 )
				.topK( 40 )
				.repeatPenalty( 1.3 )
				.build();

			Prompt visionPrompt = new Prompt( List.of( userMessage ), options );

			String footFeatures = chatModel.call( visionPrompt ).getResult().getOutput().getText();

			return footFeatures.trim();
		} )
		.subscribeOn( Schedulers.boundedElastic() )
		.flatMapMany( footFeatures -> {
			// [1번 요구사항] 이미지 인식 실패 시 정형화된 촬영 가이드라인 즉시 반환 (추천 로직 건너뜀)
			if ( footFeatures.contains( "REJECT_IMAGE" ) || footFeatures.length() < 5 ) {
				String guideMessage = """
					⚠️ **죄송합니다. 제공해주신 사진으로는 발의 특징을 정확하게 분석하기 어렵습니다.**
					
					정확한 암벽화 추천을 위해 아래 가이드라인을 참고하여 사진을 다시 촬영해 주세요!
					
					### 📸 발 사진 촬영 가이드라인
					
					1. 밝은 조명 아래 바닥에 **흰 종이(A4 용지 등)**를 깔고 그 위에 발을 올려놓고 촬영해 주세요.
					2. 카메라를 수직 위(탑뷰) 및 대각선 옆면에서 각각 촬영해 주시면 가장 정확합니다.
					3. 발의 윤곽선과 **발가락 배열(길이 관계)**이 흐릿하지 않고 명확하게 나타나야 합니다.
					
					다시 시도해 주시면 최고의 암벽화를 찾아드리겠습니다! 😊
		        """;
				return Flux.just( guideMessage );
			}

			List<Document> similarShoes = vectorStore.similaritySearch( footFeatures );
			String context = similarShoes.stream()
				.map( Document::getText )
				.collect( Collectors.joining( "\n\n" ) );

			String systemPromptText = """
	            # 역할 (Role)
	            너는 클라이밍 장비 및 암벽화 분석 전문가이자, 사용자의 발 분석 정보(Foot Features)를 기반으로 가장 알맞은 암벽화를 추천해 주는 대화형 AI 가이드이다.
	
	            # 답변 원칙 및 제약 조건 (Rules & Constraints)
	            1. **정확성 및 출처 기반:**
	               - 철저하게 제공된 [검색된 암벽화 데이터]에 명시된 브랜드, 모델명, 족형(대칭/비대칭), 발볼/토박스/힐컵 사이즈, 패드 타입(Hard/Medium/Soft), 주요 용도(볼더링/리드/올라운드) 정보를 바탕으로 답변하라.
	               - 제공된 컨텍스트에 없는 스펙이나 특징을 임의로 지어내어 설명(Hallucination)해서는 절대 안 된다. 데이터가 부족하다면 솔직하게 정보가 없음을 밝혀라.
	
	            2. **개인화된 분석 (족형 및 용도 매칭):**
	               - 사용자의 [발 분석 정보] 데이터(발 형태, 발볼 넓이, 발등 높이)를 [검색된 암벽화 데이터]와 정밀하게 대조하여, 어떤 부분이 유저의 족형에 잘 맞고 어떤 부분을 주의해야 하는지 디테일하게 비교 분석하라.
	
	            3. **가독성 높은 출력 포맷:**
	               - 사용자가 정보를 한눈에 파악할 수 있도록 **Markdown** 문법을 적극적으로 활용하라.
	               - 추천 모델 이름은 `### [브랜드] 모델명` 형태로 강조하고, 핵심 스펙은 **불릿 포인트(*)**나 **테이블(표)**을 사용하여 구조화하라.
	
	            4. **친절하고 전문적인 톤앤매너:**
	               - 클라이머의 관점에서 공감대를 형성할 수 있도록 친절하면서도 전문적인 어조를 유지하라. (예: "힐훅 걸 때 안정감이 좋습니다", "스미어링 시 부드러운 패드 타입이 유리합니다" 등 전문 용어 활용)
            """;

			String userPromptTemplateText = """
	            # [발 분석 정보]
	            {footFeatures}
	
	            # [검색된 암벽화 데이터 (Elasticsearch)]
	            {context}
	
	            ---
	            위의 [발 분석 정보]와 [검색된 암벽화 데이터]를 바탕으로, 사용자에게 가장 적합한 암벽화 추천 리스트를 작성해줘.
	            
	            # 답변 출력 가이드라인 (Markdown Format)
		        ---
		        (인사말 및 분석 요약: 유저의 발 형태, 발볼, 발등 상태를 구체적으로 언급하며 친근하게 시작)
		
		        ### 👟 [브랜드] 모델명
		        - **주요 용도:** 볼더링 / 올라운드 / 리드 등
		        - **특징 및 추천 이유:** (유저의 족형 특성과 암벽화의 족형/경도/다운토 유무를 엮어서 설명)
		        - **사이즈 팁 및 주의점:** (유저의 발볼/발등 대비 이 신발의 토박스나 압박감을 고려하여 서술)
		
		        (추가 추천 모델이 있다면 동일하게 `### 👟 [브랜드] 모델명` 형식으로 이어서 작성)
		
		        (추가 조언 및 마무리 인사)
		        ---
            """;

			SystemPromptTemplate systemMessageTemplate = new SystemPromptTemplate( systemPromptText );

			Message systemMessage = systemMessageTemplate.createMessage();

			UserMessage userMessage = new UserMessage(
				new PromptTemplate( userPromptTemplateText ).render( Map.of(
					"footFeatures", footFeatures,
					"context", context
				))
			);

			OllamaChatOptions options = OllamaChatOptions.builder()
				.model( "gemma4:e4b" )
				.temperature( 0.5 )
				.build();

			Prompt chatPrompt = new Prompt( List.of( systemMessage, userMessage ), options );

			return chatModel.stream( chatPrompt )
				.map( chunk -> {
					if ( chunk == null || chunk.getResult() == null || chunk.getResult().getOutput() == null ) {
						return "";
					}
					String content = chunk.getResult().getOutput().getText();
					if ( content == null ) return "";

					return content.replace( " ", "[SPACE]" );
				} );
		} );
	}
}