package kr.co.vincent.rag.controller;

import kr.co.vincent.rag.dto.LoginRequest;
import kr.co.vincent.rag.dto.RegisterRequest;
import kr.co.vincent.rag.dto.User;
import kr.co.vincent.rag.repository.UserRepository;
import kr.co.vincent.rag.security.CustomUserDetailsService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/auth")
@RequiredArgsConstructor
public class AuthenticationController {

	private final AuthenticationManager authenticationManager;
	private final CustomUserDetailsService userDetailsService;
	private final PasswordEncoder passwordEncoder;
	private final UserRepository userRepository;

	@PostMapping("/login")
	public ResponseEntity<Map<String, Object>> login(@RequestBody LoginRequest request) {
		try {
			Authentication authentication = authenticationManager.authenticate(
				new UsernamePasswordAuthenticationToken(
					request.getEmail(),
					request.getPassword()
				)
			);

			SecurityContextHolder.getContext().setAuthentication(authentication);

			Map<String, Object> response = new HashMap<>();
			response.put("success", true);
			response.put("message", "로그인이 성공했습니다.");
			response.put("role", authentication.getAuthorities().stream()
				.findFirst()
				.map(grantedAuthority -> grantedAuthority.getAuthority().replace("ROLE_", ""))
				.orElse("UNKNOWN"));

			return ResponseEntity.ok(response);

		} catch (BadCredentialsException e) {
			Map<String, Object> errorResponse = new HashMap<>();
			errorResponse.put("success", false);
			errorResponse.put("message", "이메일 또는 비밀번호가 잘못되었습니다.");
			return ResponseEntity.status(401).body(errorResponse);
		}
	}

	@PostMapping("/register")
	public ResponseEntity<Map<String, Object>> register(@RequestBody RegisterRequest request) {
		if (userRepository.existsByEmail(request.getEmail())) {
			Map<String, Object> errorResponse = new HashMap<>();
			errorResponse.put("success", false);
			errorResponse.put("message", "이미 존재하는 이메일입니다.");
			return ResponseEntity.badRequest().body(errorResponse);
		}

		String encodedPassword = passwordEncoder.encode(request.getPassword());
		User newUser = new User(request.getEmail(), encodedPassword, "USER");

		userRepository.save(newUser);

		Map<String, Object> response = new HashMap<>();
		response.put("success", true);
		response.put("message", "회원가입이 완료되었습니다.");
		response.put("role", "USER");

		return ResponseEntity.ok(response);
	}
}