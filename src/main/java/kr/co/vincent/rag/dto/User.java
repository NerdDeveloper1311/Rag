package kr.co.vincent.rag.dto;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Entity
@Table( name = "users" )
@Data
@NoArgsConstructor
@AllArgsConstructor
public class User {

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private Long id;

	@Column(nullable = false, unique = true)
	private String email;

	@Column(nullable = false)
	private String password;

	@Column(nullable = false)
	private String role; // ADMIN, USER

	@Column(nullable = false)
	private boolean isActive = true;

	@Column(name = "created_at")
	private LocalDateTime createdAt;

	@Column(name = "updated_at")
	private LocalDateTime updatedAt;

	public User(String email, String password, String role) {
		this.email = email;
		this.password = password;
		this.role = role;
		this.isActive = true;
	}

	public void changePassword(String newPassword) {
		this.password = newPassword;
		this.updatedAt = LocalDateTime.now();
	}

}
