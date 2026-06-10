package kr.co.vincent.rag.service;

import kr.co.vincent.rag.dto.ShoeRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.chat.messages.SystemMessage;
import org.springframework.ai.chat.messages.UserMessage;
import org.springframework.ai.chat.prompt.Prompt;
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
import org.springframework.util.MimeType;
import org.springframework.util.MimeTypeUtils;
import org.springframework.web.multipart.MultipartFile;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.ByteArrayInputStream;
import java.io.File;
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

	public Mono<Void> ingestDocument( String content ) {
		return Mono.fromRunnable( () -> {
			Document doc = new Document( content );
			vectorStore.accept( List.of( doc ) );
			log.info( "Document ingested successfully: {}", content );
		} ).subscribeOn( Schedulers.boundedElastic() ).then();
	}

	public Mono<Void> ingestFile( MultipartFile file ) {
		return Mono.fromRunnable( () -> {
			File tempFile = null;
			try {
				tempFile = File.createTempFile( "rag_upload_", "_" + file.getOriginalFilename() );
				file.transferTo( tempFile );
				Resource resource = new FileSystemResource( tempFile );

				TikaDocumentReader reader = new TikaDocumentReader( resource );
				List<Document> rawDocuments = reader.get();

				TokenTextSplitter splitter = new TokenTextSplitter();
				List<Document> splitDocuments = splitter.apply( rawDocuments );

				for ( Document doc : splitDocuments ) {
					doc.getMetadata().put( "source", file.getOriginalFilename() );
				}

				vectorStore.accept( splitDocuments );
				log.info( "Successfully ingested file: {}, total chunks: {}", file.getOriginalFilename(), splitDocuments.size() );

			} catch ( IOException e ) {
				log.error( "Failed to process file ingestion", e );
				throw new RuntimeException( "파일 처리 중 오류가 발생했습니다.", e );
			} finally {
				if ( tempFile != null && tempFile.exists() ) {
					tempFile.delete();
				}
			}
		} ).subscribeOn( Schedulers.boundedElastic() ).then();
	}

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

				// [수정] 일반 텍스트 주입 지식 및 주입된 암벽화 스펙(2번탭) 지식 모두에 유연하게 융합 답변을 할 수 있도록 프롬프트 지침 고도화
				String systemPromptText = """
					당신은 사용자가 연동한 커스텀 지식 문서 및 주입된 암벽화 스펙 데이터베이스를 기반으로 답변하는 유능한 AI 어시스턴트입니다.
					반드시 제공된 [컨텍스트] 정보(일반 문서 데이터 및 암벽화 관련 명세 지식 포함)를 최우선 바탕으로 하여 질문에 정확하게 답변해야 합니다.
					
					[지침]
					1. 제공된 [컨텍스트]에 기술되어 있는 정보와 스펙 매칭 지식을 유기적으로 연결하여 답변을 구성하세요.
					2. 컨텍스트만으로 질문에 대한 답을 명확하게 도출할 수 없다면, 억지로 허위 사실을 지어내지 말고 "제공된 정보 내에서 관련 답변을 찾을 수 없습니다." 라고 안내하십시오.
					3. 답변할 때는 문맥상 흐름이 자연스럽도록 결론 및 명확한 팩트 위주로 대답하세요.
					
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

	public Mono<Void> ingestShoeData( ShoeRequest shoe ) {
		return Mono.fromRunnable( () -> {
			StringBuffer bf = new StringBuffer();
			bf.append( "암벽화 브랜드: " + shoe.getBrand() + "\n" );
			bf.append( "모델명: " + shoe.getModel() + "\n" );
			bf.append( "족형: " + shoe.getFootShape() + "\n" );
			bf.append( "발 볼 사이즈: " + shoe.getWallSize() + "\n" );
			bf.append( "패드 타입: " + shoe.getPadType() + "\n" );
			bf.append( "패드 종류: " + shoe.getPadClass() + "\n" );
			bf.append( "토 박스 크기: " + shoe.getToeBoxSize() + "\n" );
			bf.append( "힐컵 사이즈: " + shoe.getHeelCupSize() + "\n" );
			bf.append( "힐컵 단단함: " + shoe.getHeelCupHardness() + "\n" );
			bf.append( "주요 용도: " + shoe.getMainUses() + "\n" );
			bf.append( "특징: " + shoe.getDescription() );

			Document doc = new Document( bf.toString() );
			doc.getMetadata().put( "type", "climbing_shoe" );
			doc.getMetadata().put( "name", shoe.getModel() );
			vectorStore.accept( List.of( doc ) );
			log.info( "Shoe data ingested: {}", shoe.getModel() );
		} ).subscribeOn( Schedulers.boundedElastic() ).then();
	}

	public Flux<String> recommendShoesByFootImage( List<MultipartFile> files ) {
		return Mono.fromCallable( () -> {
				if ( files == null || files.isEmpty() ) return "REJECT_IMAGE";

				StringBuilder integratedFeatures = new StringBuilder();
				int imageIndex = 1;

				// 1. 프롬프트 구조화 및 가이드라인 출력 조건 명시
				String promptText = """
                당신은 발 이미지를 판별하는 전문 분석가입니다. 
                제공된 여러 장의 발 이미지를 종합하여 발의 형태, 발볼, 발등을 분석하십시오.
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

				OllamaChatOptions options = OllamaChatOptions.builder()
					.model( "llama3.2-vision:11b" )
					.temperature( 0.3 )       // 분석 정확도를 위해 온도를 더 낮춤
					.topP( 0.85 )
					.topK( 40 )
					.repeatPenalty( 1.3 )
					.build();

				for ( MultipartFile file : files ) {
					if ( file.isEmpty() ) continue;

					String contentType = file.getContentType();
					MimeType mimeType = ( contentType != null ) ? MimeType.valueOf( contentType ) : MimeTypeUtils.IMAGE_JPEG;

					UserMessage userMessage = UserMessage.builder()
						.text( promptText )
						.media( new Media( mimeType, new ByteArrayResource( file.getBytes() ) ) )
						.build();

					Prompt visionPrompt = new Prompt( List.of( userMessage ), options );

					String singleAnalysis = chatModel.call( visionPrompt ).getResult().getOutput().getText();
					log.info( "[이미지 {}번 분석 결과]: {}", imageIndex, singleAnalysis );

					if ( singleAnalysis.contains( "REJECT_IMAGE" ) ) {
						return "REJECT_IMAGE";
					}

					integratedFeatures.append("[사진 ").append(imageIndex).append("번 분석 결과]\n")
						.append(singleAnalysis.trim()).append("\n\n");
					imageIndex++;
				}

				return integratedFeatures.toString().trim();
			} )
			.subscribeOn( Schedulers.boundedElastic() )
			.flatMapMany( footFeatures -> {
				// [1번 요구사항] 이미지 인식 실패 시 정형화된 촬영 가이드라인 즉시 반환 (추천 로직 건너뜀)
				if ( footFeatures.contains( "REJECT_IMAGE" ) || footFeatures.length() < 5 ) {
					String guideMessage = """
                    ⚠️ **죄송합니다. 제공해주신 사진으로는 발의 특징을 정확하게 분석하기 어렵습니다.**
                    
                    정확한 암벽화 추천을 위해 아래 가이드라인을 참고하여 사진을 다시 촬영해 주세요!
                    
                    ### 📸 발 사진 촬영 가이드라인
                    1. **밝은 조명** 아래 바닥에 **흰 종이(A4 용지 등)**를 깔고 그 위에 발을 올려놓고 촬영해 주세요.
                    2. 카메라를 **수직 위(탑뷰)** 및 **대각선 옆면**에서 각각 촬영해 주시면 가장 정확합니다.
                    3. 발의 **윤곽선**과 **발가락 배열(길이 관계)**이 흐릿하지 않고 명확하게 나타나야 합니다.
                    
                    다시 시도해 주시면 최고의 암벽화를 찾아드리겠습니다! 😊
                """;
					return Flux.just( guideMessage );
				}

				List<Document> similarShoes = vectorStore.similaritySearch( footFeatures );
				String context = similarShoes.stream()
					.map( Document::getText )
					.collect( Collectors.joining( "\n\n" ) );

				// [2번 요구사항] 최종 답변의 가독성을 높이기 위한 시스템 프롬프트 수정
				String systemPromptText = """
                당신은 전문 암벽화 추천 AI 전문가입니다.
                반드시 한국어로 자연스럽고 정중하게 답변하십시오.
                
                사용자가 제공한 여러 장의 사진 분석 결과([발 특징])들을 종합적으로 검토하여, [암벽화 데이터베이스]와 비교 후 가장 알맞은 암벽화를 추천하고 그 이유를 상세히 설명해주세요.
                만약 데이터베이스에 적합한 신발이 없다면 솔직하게 안내하십시오.
                
                [출력 가이드라인 (반드시 준수)]
                사용자가 읽기 편하도록 Markdown 문법을 적극적으로 활용하여 답변을 구조화하세요.
                - ##, ### 등의 헤더를 사용하여 섹션을 나누세요.
                - 추천하는 상품명은 **볼드체**로 강조하세요.
                - 추천 이유, 사이즈 팁 등은 불릿 포인트(`-`)나 번호 매기기를 활용하여 가독성을 높이세요.
                - 마지막에는 친절한 마무리 인사를 덧붙여주세요.
                - 용어 설명 표를 작성할 때는 각 행의 내용을 생략하거나 말을 흐리지 말고 반드시 완성된 문장으로 작성하세요.
				- 단어나 문장이 중간에 끊기지 않도록 문맥의 흐름을 자연스럽게 유지하세요.
				- '영어어', '손홀' 등 존재하지 않는 축약어를 만들지 마세요.
                
                [발 특징]
                {footFeatures}
                
                [암벽화 데이터베이스]
                {context}
            """;

				SystemPromptTemplate template = new SystemPromptTemplate( systemPromptText );
				String formattedPrompt = template.render( Map.of(
					"footFeatures", footFeatures,
					"context", context
				) );

				Prompt chatPrompt = new Prompt( formattedPrompt );

				return chatModel.stream( chatPrompt )
					.map( chunk -> {
						if ( chunk == null || chunk.getResult() == null || chunk.getResult().getOutput() == null ) {
							return "";
						}
						String content = chunk.getResult().getOutput().getText();

						// 데이터가 비어있거나 NULL 일 때 예외 처리
						if ( content == null ) return "";

						// 로컬 LLM에서 간혹 발생하는 특수 제어 문자나 비정상 공백 제거
						return content.replace( "\uFFFD", "" ); // 깨진 문자 기호 제거
					} );
			} );
	}
}