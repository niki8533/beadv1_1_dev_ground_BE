package io.devground.gateway.filter;

import static org.assertj.core.api.Assertions.*;

import java.util.Date;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.cloud.gateway.filter.GatewayFilter;
import org.springframework.http.HttpCookie;
import org.springframework.http.HttpStatus;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.server.ServerWebExchange;

import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import reactor.core.publisher.Mono;

class GatewayFilterTest {

	// HS256 키는 32바이트 이상이어야 함
	private static final String SECRET = "test-secret-key-for-gateway-filter-test!!";

	private GatewayFilter customFilter;
	private GatewayFilter loginFilter;
	private GatewayFilter adminFilter;

	@BeforeEach
	void setUp() {
		CustomAuthFilter custom = new CustomAuthFilter();
		LoginEssentialFilter login = new LoginEssentialFilter();
		AdminFilter admin = new AdminFilter();

		// @Value 필드는 Spring 없이 생성하면 비어있어서 직접 주입
		ReflectionTestUtils.setField(custom, "secretKey", SECRET);
		ReflectionTestUtils.setField(login, "secretKey", SECRET);
		ReflectionTestUtils.setField(admin, "secretKey", SECRET);

		customFilter = custom.apply(new CustomAuthFilter.Config());
		loginFilter = login.apply(new LoginEssentialFilter.Config());
		adminFilter = admin.apply(new AdminFilter.Config());
	}

	@Nested
	@DisplayName("로그인 안 한 경우")
	class NotLoggedIn {

		@Test
		@DisplayName("일반 필터 통과")
		void customFilter_pass() {
			MockServerWebExchange exchange = request(null);

			ServerWebExchange passed = filter(customFilter, exchange);

			assertThat(passed).isNotNull();
			assertThat(passed.getRequest().getHeaders().containsKey("X-CODE")).isFalse();
		}

		@Test
		@DisplayName("로그인 필수 필터 차단")
		void loginFilter_block() {
			MockServerWebExchange exchange = request(null);

			ServerWebExchange passed = filter(loginFilter, exchange);

			assertThat(passed).isNull();
			assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
		}

		@Test
		@DisplayName("관리자 필터 차단")
		void adminFilter_block() {
			MockServerWebExchange exchange = request(null);

			ServerWebExchange passed = filter(adminFilter, exchange);

			assertThat(passed).isNull();
			assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
		}

		@Test
		@DisplayName("access 없이 refresh 쿠키만 있으면 재발급으로 리다이렉트")
		void loginFilter_refreshOnly_redirectToReissue() {
			MockServerWebExchange exchange = MockServerWebExchange.from(
				MockServerHttpRequest.get("/api/test").cookie(new HttpCookie("refresh", "refresh-token")));

			ServerWebExchange passed = filter(loginFilter, exchange);

			assertThat(passed).isNull();
			assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.TEMPORARY_REDIRECT);
			assertThat(exchange.getResponse().getHeaders().getLocation().toString()).startsWith("/reissue");
		}
	}

	@Nested
	@DisplayName("로그인 한 경우 - 일반인")
	class LoggedInUser {

		private final String token = token("user-1", "USER");

		@Test
		@DisplayName("일반 필터 통과")
		void customFilter_pass() {
			ServerWebExchange passed = filter(customFilter, request(token));

			assertThat(passed).isNotNull();
			assertThat(passed.getRequest().getHeaders().getFirst("X-CODE")).isEqualTo("user-1");
			assertThat(passed.getRequest().getHeaders().getFirst("ROLE")).isEqualTo("USER");
		}

		@Test
		@DisplayName("로그인 필수 필터 통과")
		void loginFilter_pass() {
			ServerWebExchange passed = filter(loginFilter, request(token));

			assertThat(passed).isNotNull();
			assertThat(passed.getRequest().getHeaders().getFirst("X-CODE")).isEqualTo("user-1");
		}

		@Test
		@DisplayName("관리자 필터 차단")
		void adminFilter_block() {
			MockServerWebExchange exchange = request(token);

			ServerWebExchange passed = filter(adminFilter, exchange);

			assertThat(passed).isNull();
			assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.FORBIDDEN);
		}
	}

	@Nested
	@DisplayName("로그인 한 경우 - 관리자")
	class LoggedInAdmin {

		private final String token = token("admin-1", "ADMIN");

		@Test
		@DisplayName("일반 필터 통과")
		void customFilter_pass() {
			ServerWebExchange passed = filter(customFilter, request(token));

			assertThat(passed).isNotNull();
			assertThat(passed.getRequest().getHeaders().getFirst("ROLE")).isEqualTo("ADMIN");
		}

		@Test
		@DisplayName("로그인 필수 필터 통과")
		void loginFilter_pass() {
			ServerWebExchange passed = filter(loginFilter, request(token));

			assertThat(passed).isNotNull();
			assertThat(passed.getRequest().getHeaders().getFirst("X-CODE")).isEqualTo("admin-1");
		}

		@Test
		@DisplayName("관리자 필터 통과")
		void adminFilter_pass() {
			ServerWebExchange passed = filter(adminFilter, request(token));

			assertThat(passed).isNotNull();
			assertThat(passed.getRequest().getHeaders().getFirst("X-CODE")).isEqualTo("admin-1");
			assertThat(passed.getRequest().getHeaders().getFirst("ROLE")).isEqualTo("ADMIN");
		}
	}

	// token이 null이면 비로그인 요청
	private MockServerWebExchange request(String token) {
		MockServerHttpRequest.BaseBuilder<?> builder = MockServerHttpRequest.get("/api/test");
		if (token != null) {
			builder.header("access", "Bearer " + token);
		}
		return MockServerWebExchange.from(builder);
	}

	// 필터를 실행하고, 다음 필터로 넘어간 exchange를 반환 (차단되면 null)
	private ServerWebExchange filter(GatewayFilter filter, MockServerWebExchange exchange) {
		AtomicReference<ServerWebExchange> passed = new AtomicReference<>();
		filter.filter(exchange, ex -> {
			passed.set(ex);
			return Mono.empty();
		}).block();
		return passed.get();
	}

	private String token(String userCode, String role) {
		return Jwts.builder()
			.claim("userCode", userCode)
			.claim("role", role)
			.expiration(new Date(System.currentTimeMillis() + 60_000))
			.signWith(Keys.hmacShaKeyFor(SECRET.getBytes()))
			.compact();
	}
}
