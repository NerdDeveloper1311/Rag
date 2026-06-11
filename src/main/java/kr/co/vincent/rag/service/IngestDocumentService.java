package kr.co.vincent.rag.service;

import kr.co.vincent.rag.dto.FeedbackRequest;
import kr.co.vincent.rag.dto.ShoeRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.document.Document;
import org.springframework.ai.reader.tika.TikaDocumentReader;
import org.springframework.ai.transformer.splitter.TokenTextSplitter;
import org.springframework.ai.vectorstore.VectorStore;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class IngestDocumentService {

	private final VectorStore vectorStore;

	public Mono<Void> ingestTextDocument( String content ) {
		return Mono.fromRunnable( () -> {
			Document doc = new Document( content );
			vectorStore.accept( List.of( doc ) );
		} ).subscribeOn( Schedulers.boundedElastic() ).then();
	}

	public Mono<Void> ingestFeedbackDocument( FeedbackRequest request ) {
		return Mono.fromRunnable( () -> {
			StringBuffer buffer = new StringBuffer();
			buffer.append( "질문: " + request.getQuestion() + "\n" );
			buffer.append( "답변: " + request.getAnswer() + "\n" );
			buffer.append( "평가: " + ( request.getScore() > 0 ? "좋아요" : "싫어요" ) );

			Document doc = new Document( buffer.toString() );
			doc.getMetadata().put( "type", "qna_evaluation" );
			doc.getMetadata().put( "evaluation", ( request.getScore() > 0 ? "좋아요" : "싫어요" ) );
			doc.getMetadata().put( "score", ( request.getScore() > 0 ? "좋아요" : "싫어요" ) );
			doc.getMetadata().put( "create_dt", LocalDateTime.now().toString() );

			vectorStore.accept( List.of( doc ) );
		} ).subscribeOn( Schedulers.boundedElastic() ).then();
	}

	public Mono<Void> ingestFileDocument( MultipartFile file ) {
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

	public Mono<Void> ingestShoeDocument( ShoeRequest shoe ) {
		return Mono.fromRunnable( () -> {
			StringBuffer bf = new StringBuffer();
			bf.append( "브랜드: " + shoe.getBrand() + "\n" );
			bf.append( "모델명: " + shoe.getModel() + "\n" );
			bf.append( "족 형: " + shoe.getFootShape() + "\n" );
			bf.append( "족 형 특징: " + shoe.getFootShapeSpec() + "\n" );
			bf.append( "발 볼 사이즈: " + shoe.getWallSize() + "\n" );
			bf.append( "패드 타입: " + shoe.getPadType() + "\n" );
			bf.append( "토 박스 크기: " + shoe.getToeBoxSize() + "\n" );
			bf.append( "힐컵 사이즈: " + shoe.getHeelCupSize() + "\n" );
			bf.append( "힐컵 단단함: " + shoe.getHeelCupHardness() + "\n" );
			bf.append( "주요 용도: " + shoe.getMainUses() + "\n" );
			bf.append( "특징: " + shoe.getDescription() );

			Document doc = new Document( bf.toString() );
			doc.getMetadata().put( "type", "climbing_shoe" );
			doc.getMetadata().put( "name", shoe.getModel() );
			vectorStore.accept( List.of( doc ) );
		} ).subscribeOn( Schedulers.boundedElastic() ).then();
	}


}
