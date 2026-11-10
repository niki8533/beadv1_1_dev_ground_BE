package io.devground.payments.payment.service;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.devground.core.commands.deposit.ChargeDeposit;
import io.devground.core.event.payment.PaymentCreatedEvent;
import io.devground.core.event.payment.PaymentCreatedFailed;
import io.devground.core.model.vo.DepositHistoryType;
import io.devground.core.model.vo.ErrorCode;
import io.devground.payments.payment.infra.DepositFeignClient;
import io.devground.payments.payment.model.dto.request.PaymentRequest;
import io.devground.payments.payment.model.dto.request.RefundRequest;
import io.devground.payments.payment.model.dto.request.TossRefundRequest;
import io.devground.payments.payment.model.dto.response.GetPaymentsResponse;
import io.devground.payments.payment.model.entity.Payment;
import io.devground.payments.payment.model.vo.PaymentConfirmRequest;
import io.devground.payments.payment.model.vo.PaymentStatus;
import io.devground.payments.payment.model.vo.PaymentType;
import io.devground.payments.payment.model.vo.TossPaymentsRequest;
import io.devground.payments.payment.repository.PaymentRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@RequiredArgsConstructor
public class PaymentServiceImpl implements PaymentService {
	private final KafkaTemplate<String, Object> kafkaTemplate;
	private final ObjectMapper objectMapper;
	private final DepositFeignClient depositFeignClient;
	private final PaymentRepository paymentRepository;
	private final HttpClient httpClient;

	@Value("${deposits.command.topic.name}")
	private String depositsCommandTopic;

	@Value("${custom.toss.secretKey}")
	private String tossPaySecretKey;

	@Value("${payments.event.topic.purchase}")
	private String paymentPurchaseEventTopic;

	@Value("${custom.toss.confirm-url}")
	private String tossPayConfirmUrl;

	@Override
	@Transactional
	public Payment process(String userCode, PaymentConfirmRequest request) {
		Long balance = depositFeignClient.getBalance(userCode).data().balance();

		if (balance >= request.amount()) {

			Payment depositPayment = pay(getPaymentRequest(userCode, PaymentType.DEPOSIT, request));

			//결제 성공 이벤트 발행
			PaymentCreatedEvent event = new PaymentCreatedEvent(
				userCode,
				request.amount(),
				DepositHistoryType.PAYMENT_INTERNAL,
				request.orderCode(),
				request.productCodes()
			);

			kafkaTemplate.send(paymentPurchaseEventTopic, event.orderCode(), event);
			return depositPayment;

		}

		PaymentCreatedFailed event = new PaymentCreatedFailed(
			request.orderCode(),
			userCode,
			"예치금 부족으로 결제에 실패했습니다."
		);
		kafkaTemplate.send(paymentPurchaseEventTopic, event.orderCode(), event);
		throw new IllegalStateException("예치금 부족하여 결제를 진행할 수 없습니다.");
		// return pay(getPaymentRequest(userCode, PaymentType.TOSS_PAYMENT, request));

	}

	private PaymentRequest getPaymentRequest(String userCode, PaymentType type, PaymentConfirmRequest request) {
		return new PaymentRequest(userCode, type, request.paymentKey(), request.orderCode(), request.amount());
	}

	private Payment handleDepositPayment(String userCode, String orderCode, Long amount) {

		Payment payment = Payment.builder()
			.userCode(userCode)
			.orderCode(orderCode)
			.amount(amount)
			.build();

		payment.setPaymentStatus(PaymentStatus.PAYMENT_COMPLETED);

		return paymentRepository.save(payment);

		//예치금 사용한다고 카프카 전송
	}

	@Override
	@Transactional
	public Payment pay(PaymentRequest request) {

		Payment payment;

		if (request.getPaymentType() == PaymentType.DEPOSIT) {
			payment = handleDepositPayment(request.getUserCode(), request.getOrderCode(), request.getAmount());
		} else if (request.getPaymentType() == PaymentType.TOSS_PAYMENT) {
			payment = handleTossPayment(request.getUserCode(), request.getOrderCode(), request.getPaymentKey(),
				request.getAmount());
		} else {
			throw new UnsupportedOperationException("결제 형식이 잘못되었습니다.");
		}

		return payment;

	}

	private Payment handleTossPayment(String userCode, String orderCode, String paymentKey, Long amount) {

		// 토스페이먼츠 결제 시도
		boolean result = processTossPayment(new TossPaymentsRequest(paymentKey, orderCode, amount.toString()));

		if (!result)
			throw new IllegalStateException("토스페이먼츠 결제에 실패하였습니다.");

		// 결제 성공시 예치금 충전 처리
		//카프카 전송
		log.info("toss 충전 userCode: {}", userCode);
		ChargeDeposit command = new ChargeDeposit(
			userCode,
			paymentKey,
			amount,
			DepositHistoryType.CHARGE_TOSS
		);

		kafkaTemplate.send(depositsCommandTopic, command);
		log.info("예치금 충전 카프카 커맨드 전송");

		// 결제 내역 저장
		Payment payment = Payment.builder()
			.userCode(userCode)
			.orderCode(orderCode)
			.amount(amount)
			.paymentKey(paymentKey)
			.build();

		payment.setPaymentStatus(PaymentStatus.PAYMENT_PENDING);
		log.info("toss 충전 userCode: {}", userCode);

		return paymentRepository.save(payment);

	}

	private boolean processTossPayment(TossPaymentsRequest request) {

		try {
			// 1. Authorization Header 생성
			String target = tossPaySecretKey + ":";

			Base64.Encoder encoder = Base64.getEncoder();
			String encryptedSecretKey = "Basic " + encoder.encodeToString(target.getBytes(StandardCharsets.UTF_8));
			// 2. 요청 데이터 구성
			Map<String, Object> requestMap = objectMapper.convertValue(request, new TypeReference<>() {
			});

			HttpRequest httpRequest = HttpRequest.newBuilder()
				.uri(URI.create(tossPayConfirmUrl))
				.header("Authorization", encryptedSecretKey)
				.header("Content-Type", "application/json")
				.POST(HttpRequest.BodyPublishers.ofByteArray(objectMapper.writeValueAsBytes(requestMap)))
				.build();

			// 4. HTTP 요청 수행
			HttpResponse<String> response = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString());

			// 5. 응답 처리
			if (response.statusCode() == HttpStatus.OK.value()) {
				log.info("결제 성공");
				return true;
			} else {
				log.error("토스페이먼츠 결제 수행 과정에서 오류가 발생하였습니다. 다시 시도하여 주시기 바랍니다. 응답코드 : {}", response.statusCode());
				log.info("response.body() = {}", response.body());
				return false;
			}
		} catch (Exception e) {
			log.error("토스페이먼츠 결제 수행 과정에서 오류가 발생하였습니다.");
			return false;
		}

	}

	@Override
	@Transactional
	public void refund(RefundRequest request) {
		PaymentStatus status = PaymentStatus.PAYMENT_REFUNDED;

		Payment payment = Payment.builder()
			.userCode(request.userCode())
			.orderCode(request.orderCode())
			.amount(request.amount())
			.build();

		payment.setPaymentStatus(status);

		paymentRepository.save(payment);
	}

	@Override
	@Transactional
	public void tossRefund(TossRefundRequest request){
		try {
			String target = tossPaySecretKey + ":";

			Base64.Encoder encoder = Base64.getEncoder();
			String encryptedSecretKey = "Basic " + encoder.encodeToString(target.getBytes(StandardCharsets.UTF_8));
			String url = String.format("https://api.tosspayments.com/v1/payments/%s/cancel", request.paymentKey());


			HttpRequest httpRequest = HttpRequest.newBuilder()
				.uri(URI.create(url))
				.header("Authorization", encryptedSecretKey)
				.header("Content-Type", "application/json")
				.method("POST", HttpRequest.BodyPublishers.ofString("{\"cancelReason\":\"예치금 충전 불가\"}"))
				.build();

			HttpResponse<String> response = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString());
			System.out.println(response.body());

			Payment payment = paymentRepository.findByPaymentKey(request.paymentKey())
				.orElseThrow(ErrorCode.PAYMENT_NOT_FOUND::throwServiceException);

			payment.setPaymentStatus(PaymentStatus.PAYMENT_REFUNDED);

			if (response.statusCode() == HttpStatus.OK.value()) {
				log.info("환불 성공");
			} else {
				log.error("토스페이먼츠 환불 수행 과정에서 오류가 발생하였습니다. 다시 시도하여 주시기 바랍니다. 응답코드 : {}", response.statusCode());
				log.info("response.body() = {}", response.body());
			}
		} catch (Exception e) {
			log.error("토스페이먼츠 환불 수행 과정에서 오류가 발생하였습니다.");
		}
	}

	@Override
	@Transactional
	public Payment confirmPayment(PaymentRequest request) {
		throw new UnsupportedOperationException("");
	}

	@Override
	public Page<GetPaymentsResponse> getPayments(String userCode, Pageable pageable) {
		Page<Payment> paymentPage = pagePaymentsByUserCode(userCode, pageable);

		List<Payment> payments = paymentPage.getContent();

		if(payments.isEmpty()){
			return Page.empty(pageable);
		}

		List<GetPaymentsResponse> content = payments.stream()
			.map(p -> new GetPaymentsResponse(
				p.getCode(),
				p.getCreatedAt(),
				p.getAmount(),
				p.getPaymentType(),
				p.getPaymentStatus()
			))
			.toList();

		return new PageImpl<>(content, pageable, paymentPage.getTotalElements());
	}

	@Override
	@Transactional
	public void applyDepositPayment(String orderCode) {
		Payment payment = getByOrderCode(orderCode);
		payment.setPaymentStatus(PaymentStatus.PAYMENT_COMPLETED);
	}

	@Override
	@Transactional
	public void cancelDepositPayment(String orderCode) {
		Payment payment = getByOrderCode(orderCode);
		payment.setPaymentStatus(PaymentStatus.PAYMENT_CANCELLED);
	}

	@Override
	@Transactional
	public void applyDepositCharge(String userCode){
		log.info("예치금 충전 완료 처리");
		Payment payment = getByUserCode(userCode);
		payment.setPaymentStatus(PaymentStatus.PAYMENT_COMPLETED);
	}

	private Payment getByOrderCode(String orderCode) {
		return paymentRepository.findByOrderCode(orderCode)
			.orElseThrow(ErrorCode.PAYMENT_NOT_FOUND::throwServiceException);
	}

	private Payment getByUserCode(String userCode) {
		return paymentRepository.findByUserCodeAndPaymentStatus(userCode, PaymentStatus.PAYMENT_PENDING)
			.orElseThrow(ErrorCode.PAYMENT_NOT_FOUND::throwServiceException);
	}

	private Page<Payment> pagePaymentsByUserCode(String userCode, Pageable pageable) {
		return paymentRepository.findByUserCodeOrderByPaidAtDesc(userCode, pageable);
	}

}
