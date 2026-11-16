package io.devground.gateway.filter;

import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.gateway.filter.GatewayFilter;
import org.springframework.cloud.gateway.filter.factory.AbstractGatewayFilterFactory;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.devground.core.model.web.BaseResponse;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jws;
import io.jsonwebtoken.JwtException;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import reactor.core.publisher.Flux;

@Component
public class LoginEssentialFilter extends AbstractGatewayFilterFactory<LoginEssentialFilter.Config> {

	@Value("${custom.jwt.secrets.app-key}")
	private String secretKey;

	private final Logger log = LoggerFactory.getLogger(this.getClass());

	private final ObjectMapper om = new ObjectMapper();

	public static class Config {
	}

	public LoginEssentialFilter() {

		super(Config.class);
	}

	@Override
	public GatewayFilter apply(Config config) {

		return (exchange, chain) -> {
			log.info("요청이 필터에 도착함: {}", exchange.getRequest().getURI());

			ServerHttpRequest request = exchange.getRequest();
			ServerHttpResponse response = exchange.getResponse();

			// access, refresh 둘 다 없음 = 로그인한 적 없는 사용자 → 401로 로그인 안내
			if (!request.getHeaders().containsKey("access") && !request.getCookies()
				.containsKey("refresh")) {

				log.info("로그인하지 않은 사용자의 접근입니다.");

				return response.writeWith(Flux.just(writeUnauthorizedResponseBody(response)));
			}

			Optional<String> tokenOptional = resolveToken(request);

			if (tokenOptional.isEmpty()) {
				// 토큰이 아예 없으면 재발급 페이지(또는 API)로 리다이렉트
				String originalUri = exchange.getRequest().getURI().toString();
				response.setStatusCode(HttpStatus.TEMPORARY_REDIRECT); // 307 리다이렉트
				String redirectUrl = URLEncoder.encode(originalUri, StandardCharsets.UTF_8);
				String reissueUrl = "/reissue?redirectUrl=" + redirectUrl;
				response.getHeaders().setLocation(URI.create(reissueUrl));
				return response.setComplete();
			}

			String token = tokenOptional.get();

			if (!isValidToken(token)) {
				// 토큰 만료나 유효하지 않은 경우에도 재발급 페이지로 리다이렉트
				String originalUri = exchange.getRequest().getURI().toString();
				response.setStatusCode(HttpStatus.TEMPORARY_REDIRECT); // 307 리다이렉트
				String redirectUrl = URLEncoder.encode(originalUri, StandardCharsets.UTF_8);
				String reissueUrl = "/reissue?redirectUrl=" + redirectUrl;
				response.getHeaders().setLocation(URI.create(reissueUrl));
				return response.setComplete();
			}
			Jws<Claims> claims = getClaims(token);

			String accountCode = claims.getPayload().get("userCode").toString();

			ServerHttpRequest mutatedRequest = request.mutate()
				.header("X-CODE", accountCode)
				.build();

			return chain.filter(exchange.mutate().request(mutatedRequest).build());

		};
	}

	private DataBuffer writeUnauthorizedResponseBody(ServerHttpResponse response) {
		response.setStatusCode(HttpStatus.UNAUTHORIZED);
		response.getHeaders().add(HttpHeaders.CONTENT_TYPE, "application/json");

		BaseResponse<Object> body = BaseResponse.fail(401, "로그인이 필요합니다.");

		try {
			return response.bufferFactory().wrap(om.writeValueAsBytes(body));
		} catch (JsonProcessingException e) {
			log.error("Serialization 오류");
			throw new RuntimeException(e);
		}
	}

	private Optional<String> resolveToken(ServerHttpRequest request) {

		String token = request.getHeaders().getFirst("access");

		if (token != null && !token.isBlank()) {
			// "Bearer "로 시작하면 잘라내고, 아니면 그대로 사용
			if (token.startsWith("Bearer ")) {
				return Optional.of(token.substring(7));
			}
			return Optional.of(token);
		}

		return Optional.empty();
	}

	private boolean isValidToken(String token) {

		try {
			getClaims(token);
			return true;
		} catch (JwtException e) {
			log.info("Invalid JWT Token was detected: {}  msg : {}", token, e.getMessage());
		} catch (IllegalArgumentException e) {
			log.info("JWT claims String is empty: {}  msg : {}", token, e.getMessage());
		} catch (Exception e) {
			log.error("an error raised from validating token : {}  msg : {}", token, e.getMessage());
		}

		return false;
	}

	private Jws<Claims> getClaims(String token) {
		return Jwts.parser()
			.verifyWith(Keys.hmacShaKeyFor(secretKey.getBytes()))
			.build()
			.parseSignedClaims(token);
	}
}
