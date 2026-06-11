package kr.co.vincent.rag.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

@Controller
public class RagWebController {

	@GetMapping(value = "/")
	public String index() {
		return "index"; // 멀티모달 전용 (Open-WebUI / Gemini 스타일)
	}

	@GetMapping(value = "/admin/embedding")
	public String embedding() {
		return "embedding"; // 암벽화 스펙 주입 및 일반 데이터 인제스션 전용
	}

	@GetMapping(value = "/chatbot")
	public String chatbot() {
		return "chatbot"; // 순수 텍스트 RAG 전용 대화창
	}
}