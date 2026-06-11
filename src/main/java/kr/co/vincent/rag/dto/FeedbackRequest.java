package kr.co.vincent.rag.dto;

import lombok.Data;

@Data
public class FeedbackRequest {

	private String question;

	private String answer;

	private int score;

	private String createdAt;

}
