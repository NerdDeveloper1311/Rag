package kr.co.vincent.rag.controller;

import kr.co.vincent.rag.service.RagService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;

@RestController
@RequestMapping( value = "/api" )
@RequiredArgsConstructor
public class RagController {

	private final RagService ragService;

	@PostMapping(value = "/rag/analyze", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
	@PreAuthorize("hasAnyRole('ADMIN', 'USER')")
	public String analyzeFoot(@RequestParam("files") List<MultipartFile> files,
	                          @RequestParam("query") String query,
	                          @AuthenticationPrincipal UserDetails userDetails) {
		return ragService.processMultimodalStream(files, query)
			.blockFirst();
	}

	@GetMapping("/rag/shoes")
	@PreAuthorize("hasAnyRole('ADMIN', 'USER')")
	public List<String> getRecommendedShoes(@RequestParam("query") String query) {
		// TODO: 실제 구현
		return List.of();
	}

}
