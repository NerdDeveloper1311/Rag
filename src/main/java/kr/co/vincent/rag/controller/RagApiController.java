package kr.co.vincent.rag.controller;

import kr.co.vincent.rag.dto.DocumentRequest;
import kr.co.vincent.rag.dto.ShoeRequest;
import kr.co.vincent.rag.service.RagService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

@RestController
@RequestMapping( value = "/api/rag" )
@RequiredArgsConstructor
public class RagApiController {

	private final RagService ragService;

	@PostMapping( value = "/ingest" )
	public Mono<String> ingext(@RequestBody DocumentRequest request ) {
		return ragService.ingestDocument( request.getContent() )
			.thenReturn( "문서가 성공적으로 임베딩되어 ELasticsearch에 저장 되었습니다." );
	}

	@PostMapping( value = "/ingest-file", consumes = MediaType.MULTIPART_FORM_DATA_VALUE )
	public Mono<String> ingestFile( @RequestPart( value = "file" ) MultipartFile file)  {
		return ragService.ingestFile( file )
			.thenReturn( "파일이 성공적으로 파싱 및 분할되어 Elasticsearch에 저장되었습니다: " + file.getOriginalFilename() );
	}

	@GetMapping( value = "/chat", produces = MediaType.TEXT_EVENT_STREAM_VALUE )
	public Flux<String> chat( @RequestParam( value = "query" ) String query ) {
		return ragService.chatWithDocument( query );
	}
	@PostMapping( value = "/ingest-shoe" )
	public Mono<String> ingestShoe( @RequestBody ShoeRequest request ) {
		return ragService.ingestShoeData( request )
			.thenReturn( "암벽화 데이터가 성공적으로 임베딩되었습니다." );
	}

	@PostMapping( value = "/recommend-shoes", consumes = MediaType.MULTIPART_FORM_DATA_VALUE, produces = MediaType.TEXT_EVENT_STREAM_VALUE )
	public Flux<String> recommendShoe( @RequestPart( "files" ) List<MultipartFile> files) {
		return ragService.recommendShoesByFootImage( files );
	}

}
