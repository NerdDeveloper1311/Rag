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

	public Mono<Void> ingestDocument( String content ) {
		return Mono.fromRunnable( () -> {
			Document doc = new Document( content );
			vectorStore.accept( List.of( doc ) );
			log.info( "Document ingested successfully: {}", content );
		} ).subscribeOn( Schedulers.boundedElastic() ).then();
	}

	public Mono<Void> ingestFile( MultipartFile file) {
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
					// 기존 프론트엔드 공백 처리 정책([SPACE])에 맞추어 리턴
					return Flux.just( "제공된[SPACE]문서에[SPACE]해당[SPACE]질문과[SPACE]관련된[SPACE]내용을[SPACE]찾을[SPACE]수[SPACE]없습니다." );
				}

				String systemPromptText = """
				당신은 질문에 답변하는 유능한 AI 어시스턴트입니다.
				반드시 제공된 [컨텍스트] 정보만을 바탕으로 질문에 정확하게 답변해야 합니다.
				
				[지침]
				1. 제공된 [컨텍스트]에 명시적으로 나와 있는 사실이나 정보만을 사용하여 답변을 구성하세요.
				2. 당신이 원래 알고 있던 상식, 외부 지식, 혹은 추측성 의견을 절대 덧붙이지 마세요.
				3. 제공된 [컨텍스트]만으로는 질문에 대한 답을 명확하게 도출할 수 없다면, 임의로 답변을 지어내지 말고 정확히 "제공된 문서에서 관련 정보를 찾을 수 없습니다." 라고만 답변하세요.
				4. 답변할 때는 "제공된 컨텍스트에 따르면" 같은 부자연스러운 서두는 생략하고 본론부터 자연스럽게 대답하세요.
				
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
			bf.append( "모델명: " + shoe.getShoeName() + "\n" );
			bf.append( "족형: " + shoe.getLast() + "\n" );
			bf.append( "강성 타입: " + shoe.getStiffness() + "\n" );
			bf.append( "힐컵 크기: " + shoe.getHillCupSize() + "\n" );
			bf.append( "주요 용도: " + shoe.getMainUses() + "\n" );
			bf.append( "특징: " + shoe.getDescription() );

			Document doc = new Document( bf.toString() );
			doc.getMetadata().put( "type", "climbing_shoe" );
			doc.getMetadata().put( "name", shoe.getShoeName() );
			vectorStore.accept( List.of( doc ) );
			log.info( "Shoe data ingested: {}", shoe.getShoeName() );
		} ).subscribeOn( Schedulers.boundedElastic() ).then();
	}

	public Flux<String> recommendShoesByFootImage( MultipartFile file ) {
		return Mono.fromCallable( () -> {
			byte[] bytes = file.getBytes();
			String promptText = "이 발 이미지의 형태(이집트형, 로마형, 그리스형 중 무엇에 가까운지), 발볼의 넓이, 발등의 높이 등 암벽화 선택에 필요한 주요 특징을 짧고 명확한 텍스트로 추출해줘. 이미지 분석이 어렵다면 발 사진 찍는 방법을 말해줘.";

			UserMessage userMessage = UserMessage.builder()
				.text(promptText)
				.media(new Media(MimeTypeUtils.IMAGE_JPEG, new ByteArrayResource(bytes)))
				.build();

			Prompt visionPrompt = new Prompt( List.of( userMessage ), OllamaChatOptions.builder().model( "llama3.2-vision:11b" ).build() );// Vision 모델 지정

			String footFeatures = chatModel.call( visionPrompt ).getResult().getOutput().getText();
			log.info( "분석된 발 특징: {}", footFeatures );

			return footFeatures;
		} )
		.subscribeOn( Schedulers.boundedElastic() )
		.flatMapMany( footFeatures -> {
			List<Document> similarShoes = vectorStore.similaritySearch( footFeatures );
			String context = similarShoes.stream()
				.map( Document::getText )
				.collect( Collectors.joining( "\n\n" ) );

			String systemPromptText = """
				당신은 전문 암벽화 추천 AI입니다.
				아래 사용자의 [발 특징]과 [암벽화 데이터베이스]를 비교하여 가장 알맞은 암벽화를 추천해주세요.
				추천 이유도 상세히 설명해주세요. 데이터베이스에 적합한 신발이 없다면 솔직하게 말해주세요.
				
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
					if ( content == null ) return "";

					return content.replace( " ", "[SPACE]" );
				} );
		} );
	}
}