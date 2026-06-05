package kr.co.vincent.rag.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.chat.messages.SystemMessage;
import org.springframework.ai.chat.messages.UserMessage;
import org.springframework.ai.chat.prompt.Prompt;
import org.springframework.ai.chat.prompt.SystemPromptTemplate;
import org.springframework.ai.document.Document;
import org.springframework.ai.ollama.OllamaChatModel;
import org.springframework.ai.reader.tika.TikaDocumentReader;
import org.springframework.ai.transformer.splitter.TokenTextSplitter;
import org.springframework.ai.vectorstore.VectorStore;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Service;
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
}
