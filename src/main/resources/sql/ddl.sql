-- RAG 프로젝트 데이터베이스 스키마
-- 생성일: 2024
-- 데이터베이스: PostgreSQL 15+

-- 1. 사용자 테이블
CREATE TABLE users (
    id BIGSERIAL PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    password VARCHAR(255) NOT NULL,
    role VARCHAR(50) NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. 족형 분석 결과 테이블
CREATE TABLE foot_analysis (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT REFERENCES users(id),
    foot_shape VARCHAR(100),
    foot_ball_width VARCHAR(50),
    foot_top_height VARCHAR(50),
    analysis_result TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. 암벽화 매칭 결과 테이블
CREATE TABLE climbing_match (
    id BIGSERIAL PRIMARY KEY,
    foot_analysis_id BIGINT REFERENCES foot_analysis(id),
    brand VARCHAR(100),
    model VARCHAR(100),
    match_score DECIMAL(5,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 4. 시스템 로그 테이블
CREATE TABLE system_logs (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT REFERENCES users(id),
    action VARCHAR(100),
    details TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 5. 인덱스 생성
CREATE INDEX idx_users_email ON users(email);
CREATE INDEX idx_foot_analysis_user ON foot_analysis(user_id);
CREATE INDEX idx_climbing_match_analysis ON climbing_match(foot_analysis_id);
CREATE INDEX idx_system_logs_user ON system_logs(user_id);

-- 6. 초기 관리자 계정 삽입
INSERT INTO users (email, password, role, is_active)
VALUES ('admin@vincent.co.kr', '$2a$10$N9qo8uLOickgx2ZMRZoMyeIjZ7c2Syh4Rq0k3q3q3q3q3q3q3', 'ADMIN', TRUE);

-- 7. 일반 사용자 계정 삽입
INSERT INTO users (email, password, role, is_active)
VALUES ('user@vincent.co.kr', '$2a$10$N9qo8uLOickgx2ZMRZoMyeIjZ7c2Syh4Rq0k3q3q3q3q3q3q3', 'USER', TRUE);