package kr.co.vincent.rag.controller;

import kr.co.vincent.rag.service.MonitorService;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;

@Controller
public class MonitorController {

	private final MonitorService monitorService;

	public MonitorController( MonitorService monitorService ) {
		this.monitorService = monitorService;
	}

	@GetMapping( value = "/monitor" )
	public String getMonitorPage(Model model) {
		model.addAttribute( "metrics", monitorService.getServerMetrics() );
		return "monitor";
	}

}
