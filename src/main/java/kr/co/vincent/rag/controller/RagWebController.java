package kr.co.vincent.rag.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

@Controller
public class RagWebController {

	@GetMapping( value = "/" )
	public String index() {
		return "index";
	}

}
