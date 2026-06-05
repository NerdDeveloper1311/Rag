package kr.co.vincent.rag.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
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
				.map( Document::getContent )
				.collect( Collectors.joining( "\n\n" ) );
		} )
		.subscribeOn( Schedulers.boundedElastic() )
		.flatMapMany( context -> {
			String systemPromptText = """
				당신은 질문에 답변하는 유능하고 자연스러운 AI 어시스턴트입니다.
				제공된 [컨텍스트] 정보를 참고하되, 답변할 때는 절대로 "제공된 컨텍스트에 따르면", "문서에 의하면" 같은 딱딱한 서두나 전제 조건을 붙이지 마세요.
				마치 원래 알고 있던 상식처럼 질문에 대해 친절하고 정확하게 '본론부터' 답변하세요.
				만약 컨텍스트에서 답을 찾을 수 없다면, 솔직하게 모른다고 답변하세요.
				
				[컨텍스트]
				{context}
				""";

			SystemPromptTemplate template = new SystemPromptTemplate( systemPromptText );
			var systemMessage = template.createMessage( Map.of( "context", context ) );
			var userMessage = new UserMessage( query );

			Prompt prompt = new Prompt( List.of( systemMessage, userMessage ) );

			return chatModel.stream( prompt )
				.map( chunk -> {
					if (chunk == null || chunk.getResult() == null || chunk.getResult().getOutput() == null) {
						return "";
					}
					String content = chunk.getResult().getOutput().getContent();
					if (content == null) return "";

					// [핵심] 공백을 안전한 특수 기호로 변환하여 SSE 전송
					return content.replace(" ", "[SPACE]");
				} );
		} );
	}
}
