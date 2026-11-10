package io.devground.user.saga;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import io.devground.core.commands.cart.CreateCartCommand;
import io.devground.core.commands.deposit.CreateDeposit;
import io.devground.core.commands.deposit.DeleteDeposit;
import io.devground.core.commands.user.NotifyUserCreateFailedAlertCommand;
import io.devground.core.event.cart.CartCreatedEvent;
import io.devground.core.event.cart.CartCreatedFailedEvent;
import io.devground.core.event.deposit.DepositCreateFailed;
import io.devground.core.event.deposit.DepositCreatedSuccess;
import io.devground.core.event.deposit.DepositDeletedSuccess;
import io.devground.core.events.user.UserCreatedEvent;
import io.devground.user.service.UserService;

@ExtendWith(MockitoExtension.class)
class UserSagaTest {

	private static final String USER_CODE = "user-1";
	private static final String USER_TOPIC = "users-join-commands";
	private static final String DEPOSIT_TOPIC = "deposits-join-commands";
	private static final String CART_TOPIC = "carts-join-commands";

	@Mock
	private UserService userService;

	@Mock
	private KafkaTemplate<String, Object> kafkaTemplate;

	@InjectMocks
	private UserSaga userSaga;

	@BeforeEach
	void setUp() {
		// @Value 필드는 Mockito가 못 채워서 직접 주입
		ReflectionTestUtils.setField(userSaga, "userJoinCommandTopicName", USER_TOPIC);
		ReflectionTestUtils.setField(userSaga, "depositsJoinUserCommandTopicName", DEPOSIT_TOPIC);
		ReflectionTestUtils.setField(userSaga, "cartsJoinUserCommandTopicName", CART_TOPIC);
	}

	@Nested
	@DisplayName("회원가입 성공 흐름")
	class Success {

		@Test
		@DisplayName("유저 생성 → 예치금 생성 커맨드 발행")
		void userCreated_sendsCreateDeposit() {
			userSaga.handleEvent(new UserCreatedEvent(USER_CODE));

			verify(kafkaTemplate).send(DEPOSIT_TOPIC, USER_CODE, new CreateDeposit(USER_CODE));
		}

		@Test
		@DisplayName("예치금 생성 성공 → 예치금 코드 저장 후 장바구니 생성 커맨드 발행")
		void depositCreated_savesCodeAndSendsCreateCart() {
			userSaga.handleEvent(new DepositCreatedSuccess(USER_CODE, "deposit-1"));

			verify(userService).applyDepositCode(USER_CODE, "deposit-1");
			verify(kafkaTemplate).send(CART_TOPIC, USER_CODE, new CreateCartCommand(USER_CODE));
		}

		@Test
		@DisplayName("장바구니 생성 성공 → 장바구니 코드 저장, 사가 종료")
		void cartCreated_savesCartCode() {
			userSaga.handleEvent(new CartCreatedEvent(USER_CODE, "cart-1"));

			verify(userService).applyCartCode(USER_CODE, "cart-1");
			verify(userService, never()).deleteByUserCode(anyString());
			verifyNoInteractions(kafkaTemplate);
		}
	}

	@Nested
	@DisplayName("회원가입 실패 흐름 (보상 트랜잭션)")
	class Failure {

		@Test
		@DisplayName("예치금 생성 실패 → 유저 삭제")
		void depositCreateFailed_deletesUser() {
			userSaga.handleEvent(new DepositCreateFailed(USER_CODE, "예치금 생성에 실패했어요"));

			verify(userService).deleteByUserCode(USER_CODE);
			verifyNoInteractions(kafkaTemplate);
		}

		@Test
		@DisplayName("장바구니 생성 실패 → 예치금 삭제 커맨드 발행 (유저는 아직 삭제 안 함)")
		void cartCreateFailed_sendsDeleteDeposit() {
			userSaga.handleEvent(new CartCreatedFailedEvent(USER_CODE, "장바구니 생성 실패"));

			verify(kafkaTemplate).send(DEPOSIT_TOPIC, USER_CODE, new DeleteDeposit(USER_CODE));
			verify(userService, never()).deleteByUserCode(anyString());
		}

		@Test
		@DisplayName("예치금 삭제 완료 → 유저 삭제 후 실패 알림 발행")
		void depositDeleted_deletesUserAndNotifies() {
			userSaga.handleEvent(new DepositDeletedSuccess(USER_CODE, "예치금 삭제 완료"));

			verify(userService).deleteByUserCode(USER_CODE);
			verify(kafkaTemplate).send(USER_TOPIC, USER_CODE, new NotifyUserCreateFailedAlertCommand(USER_CODE));
		}
	}
}
