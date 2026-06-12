package kr.co.vincent.rag.dto;

import lombok.Data;

@Data
public class RegisterRequest {

	private String email;
	private String password;
	private String role;

}
