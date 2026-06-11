package kr.co.vincent.rag.controller;

import kr.co.vincent.rag.dto.DocumentRequest;
import kr.co.vincent.rag.dto.FeedbackRequest;
import kr.co.vincent.rag.dto.ShoeRequest;
import kr.co.vincent.rag.service.IngestDocumentService;
import kr.co.vincent.rag.service.RagService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

@RestController
@RequestMapping(value = "/api/rag")
@RequiredArgsConstructor
public class RagApiController {

	private final RagService ragService;
	private final IngestDocumentService ingestDocumentService;

	@PostMapping(value = "/ingest")
	public Mono<String> ingest(@RequestBody DocumentRequest request) {
		return ingestDocumentService.ingestTextDocument(request.getContent())
			.thenReturn("문서가 성공적으로 임베딩되어 Elasticsearch에 저장 되었습니다.");
	}

	@PostMapping(value = "/ingest-file", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
	public Mono<String> ingestFile(@RequestParam(value = "file") MultipartFile file) {
		return ingestDocumentService.ingestFileDocument(file)
			.thenReturn("파일이 성공적으로 파싱 및 분할되어 Elasticsearch에 저장되었습니다: " + file.getOriginalFilename());
	}

	@PostMapping(value = "/ingest-shoe")
	public Mono<String> ingestShoe(@RequestBody ShoeRequest request) {
		return ingestDocumentService.ingestShoeDocument(request)
			.thenReturn("암벽화 데이터가 성공적으로 임베딩되었습니다.");
	}

	@PostMapping(value = "/feedback")
	public Mono<ResponseEntity<String>> ingestFeedback(@RequestBody FeedbackRequest request) {
		return ingestDocumentService.ingestFeedbackDocument(request)
			.thenReturn(ResponseEntity.ok("피드백 데이터 벡터화 및 저장 완료"));
	}

	@PostMapping(value = "/chat/multimodal", consumes = MediaType.MULTIPART_FORM_DATA_VALUE, produces = MediaType.TEXT_EVENT_STREAM_VALUE)
	public Flux<String> multimodalChat(
		@RequestParam(value = "files", required = false) List<MultipartFile> files,
		@RequestParam(value = "query", required = false) String query) {
		return ragService.processMultimodalStream(files, query);
	}
}