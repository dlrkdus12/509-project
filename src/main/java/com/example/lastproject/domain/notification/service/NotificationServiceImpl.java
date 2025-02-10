package com.example.lastproject.domain.notification.service;

import com.example.lastproject.common.dto.AuthUser;
import com.example.lastproject.common.enums.ErrorCode;
import com.example.lastproject.common.exception.CustomException;
import com.example.lastproject.domain.chat.dto.ChatRoomResponse;
import com.example.lastproject.domain.likeitem.dto.response.LikeItemResponse;
import com.example.lastproject.domain.likeitem.repository.LikeItemQueryRepository;
import com.example.lastproject.domain.notification.dto.NotificationListResponse;
import com.example.lastproject.domain.notification.dto.NotificationResponse;
import com.example.lastproject.domain.notification.entity.Notification;
import com.example.lastproject.domain.notification.entity.NotificationType;
import com.example.lastproject.domain.notification.kafka.service.KafkaProducerService;
import com.example.lastproject.domain.notification.repository.EmitterRepository;
import com.example.lastproject.domain.notification.repository.NotificationRepository;
import com.example.lastproject.domain.party.entity.Party;
import com.example.lastproject.domain.party.repository.PartyQueryRepositoryImpl;
import com.example.lastproject.domain.party.repository.PartyRepository;
import com.example.lastproject.domain.user.dto.NearbyBookmarkUserDto;
import com.example.lastproject.domain.user.entity.User;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RequiredArgsConstructor
@Transactional(readOnly = true)
@Service
@Slf4j
public class NotificationServiceImpl implements NotificationService {

    private final EmitterRepository emitterRepository;
    private final PartyRepository partyRepository;
    private final PartyQueryRepositoryImpl partyQueryRepository;
    private final LikeItemQueryRepository likeItemQueryRepository;
    private final NotificationRepository notificationRepository;
    private final KafkaProducerService kafkaProducer;

    // 연결 지속시간 30분
    private static final Long DEFAULT_TIMEOUT = 30 * 60 * 1000L;

    @Value("${client.basic-url}")
    private String clientBasicUrl;

    /**
     * SSE 연결: 클라이언트가 마지막으로 수신한 데이터를 기준으로 유실된 데이터를 다시 전송하거나 최초 연결 시 더미 데이터를 전송합니다.
     *
     * @param authUser    요청을 보낸 인증된 사용자 정보
     * @param lastEventId 클라이언트가 마지막으로 수신한 데이터의 Id값을 의미한다. 이를 이용하여 유실된 데이터를 다시 보내줄 수 있다.
     * @return SseEmitter(발신기)를 생성하여 반환합니다.
     */
    @Override
    public SseEmitter subscribe(AuthUser authUser, String lastEventId) {
        String emitterId = makeTimeIncludeId(authUser);
        SseEmitter emitter = emitterRepository.save(emitterId, new SseEmitter(DEFAULT_TIMEOUT));

        // SSE 완료될 때
        emitter.onCompletion(() -> {
            emitterRepository.deleteById(emitterId);
            log.info("SseEmitter connection completed and deleted: {}", emitterId);
        });

        // SSE 타임아웃될 때
        emitter.onTimeout(() -> {
            emitterRepository.deleteById(emitterId);
            log.warn("SseEmitter connection timed out and deleted: {}", emitterId);
        });

        // SSE 에러 발생 시
        emitter.onError((e) -> {
            emitterRepository.deleteById(emitterId);
            log.error("SseEmitter connection error: {}, error: {}", emitterId, e.getMessage());
        });

        // 이벤트 발생시 lastEventId가 있을 때 이 구문을 통과
        if (!lastEventId.isEmpty()) {
            Map<String, Object> events = emitterRepository.findAllEventCacheStartWithByUserId(String.valueOf(authUser.getUserId()));

            // events.entrySet()은 events 맵의 **모든 항목(키, 값)**을 순차적으로 가져옴
            List<NotificationResponse> responseList = events.entrySet().stream()
                    .filter(entry -> lastEventId.compareTo(entry.getKey()) < 0)
                    .map(entry -> NotificationResponse.of((Notification) entry.getValue()))
                    .collect(Collectors.toList());

            String eventId = makeTimeIncludeId(authUser); // 새로 생성된 이벤트 ID
            sendToClient(emitter, emitterId, eventId, responseList);
        }
        // 처음 sse 연결 (lastEventId가 비어있을 때)
        else {
            String eventId = makeTimeIncludeId(authUser);
            NotificationResponse dummyResponse = NotificationResponse.of("eventStream. [userId=" + authUser.getUserId() + "]");
            sendToClient(emitter, emitterId, eventId, List.of(dummyResponse));
        }
        return emitter;
    }

    /**
     * 데이터 유실 시점을 파악하기 위해 사용자 ID와 현재 시간을 포함한 ID를 생성합니다.
     *
     * @param authUser 인증된 사용자 정보
     * @return 사용자 ID와 현재 시간이 포함된 문자열 ID
     */
    private String makeTimeIncludeId(AuthUser authUser) {
        return authUser.getUserId() + "_" + System.currentTimeMillis();
    }

    /**
     * 클라이언트에게 데이터를 전송합니다. SseEmitter를 사용하여 SSE 이벤트를 발송합니다.
     *
     * @param emitter   SseEmitter 객체
     * @param emitterId 발신기 ID
     * @param eventId   이벤트 ID
     * @param data      전송할 데이터 목록
     */
    private void sendToClient(SseEmitter emitter, String emitterId, String eventId, List<NotificationResponse> data) {
        try {
            for (NotificationResponse notification : data) {

                emitter.send(SseEmitter
                        .event()
                        .name("SSE") // 이벤트 이름
                        .id(eventId) // 이벤트 ID
                        .data(notification)); // 개별 객체 전송
            }
        } catch (IOException exception) {
            if (exception.getMessage().contains("Broken pipe")) {
                log.warn("Client disconnected prematurely - emitterId: {}", emitterId);
            } else {
                log.error("SSE 전송 실패 - emitterId: {}, error: {}", emitterId, exception.getMessage());
            }
            emitterRepository.deleteById(emitterId);
        }
    }

    /**
     * 주어진 알림 목록을 저장한 후, 해당 알림을 지정된 사용자에게 전송합니다.
     *
     * @param receiverId    알림을 받을 사용자 ID
     * @param notifications 전송할 알림 목록
     */
    @Override
    public void send(Long receiverId, List<Notification> notifications) {
        List<Notification> savedNotifications = saveNotifications(notifications);
        sendNotifications(receiverId, savedNotifications);
    }

    /**
     * 비동기 방식으로 사용자의 SSE Emitter에 알림을 전송합니다.
     * 알림을 전송하기 전에, 알림을 캐시에 저장하여 유실 시 복구할 수 있도록 합니다.
     *
     * @param receiverId    알림을 받을 사용자 ID
     * @param notifications 전송할 알림 목록
     */
    @Async
    @Override
    public void sendNotifications(Long receiverId, List<Notification> notifications) {
        String receiverKey = String.valueOf(receiverId);

        Map<String, SseEmitter> emitters = emitterRepository.findAllEmitterStartWithByUserId(receiverKey);

        // 각 Emitter에 대해 알림 리스트 전송
        emitters.forEach(
                (key, emitter) -> {
                    String eventId = receiverKey + "_" + System.currentTimeMillis();

                    // 알림 데이터를 캐시에 개별적으로 저장 (유실된 데이터 복구 목적)
                    notifications.forEach(
                            notification -> emitterRepository.saveEventCache(key, notification)
                    );

                    // 📌 실제 알림 리스트를 NotificationResponse 객체로 변환
                    List<NotificationResponse> responses = notifications.stream()
                            .map(NotificationResponse::of)
                            .toList();

                    sendToClient(emitter, key, eventId, responses);
                });
    }

    /**
     * 알림 목록을 저장합니다.
     *
     * @param notifications 저장할 알림 목록
     * @return 저장된 알림 목록
     */
    @Transactional
    @Override
    public List<Notification> saveNotifications(List<Notification> notifications) {
        return notificationRepository.saveAll(notifications);
    }

    /**
     * 사용자가 찜한 품목에 대한 파티가 생성된 경우 해당 사용자에게 알림을 보냅니다.
     *
     * @param authUser 요청을 보낸 인증된 사용자 정보
     * @param partyId  생성된 파티의 ID
     */
    @Transactional
    @Override
    public void notifyUsersAboutPartyCreation(AuthUser authUser, Long partyId) {
        User.fromAuthUser(authUser);
        Party party = validatePartyExists(partyId);

        // 반경 10km 이내의 즐겨찾기 유저 조회
        List<NearbyBookmarkUserDto> nearbyUsers = partyQueryRepository.getUserIdWithDistanceNearbyParty(
                party.getLatitude(),
                party.getLongitude(),
                party.getItem().getId()
        );
//        log.info("Nearby users: {}", nearbyUsers);

        // 주변 유저가 없으면 알림을 보내지 않음
        if (nearbyUsers.isEmpty()) {
//            log.info("10km 이내에 유저가 없습니다. 알림을 건너뜁니다.");
            return; // 알림 대상이 없으면 종료
        }

        // 파티 생성한 유저의 찜한 품목 조회
        List<LikeItemResponse> bookmarkedItems = likeItemQueryRepository.getBookmarkedItems(authUser.getUserId());

        // 찜한 품목이 없으면 알림을 보내지 않음
        if (bookmarkedItems.isEmpty()) {
//            log.info("찜한 품목이 없습니다. 알림을 건너뜁니다.");
            return;
        }

        // SSE 메시지 구성
        String message = String.format("Created. %s %s %s",
                party.getMarketAddress(),
                party.getMarketName(),
                party.getItem().getCategory()
        );

        String notificationUrl = String.format("%s/parties/%d", clientBasicUrl, partyId); // URL 생성

        // 알림 생성 및 저장
        List<Notification> notifications = nearbyUsers.stream()
                .map(userDto -> Notification.builder()
                        .notificationType(NotificationType.PARTY_CREATE) // 알림 타입 설정
                        .content(message) // 알림 내용
                        .url(notificationUrl) // 알림 URL
                        .receiverId(userDto.getUserId()) // 알림을 받을 유저 설정
                        .isRead(false) // 기본값 설정 (읽지 않음)
                        .build()
                )
                .toList();

        // 알림을 유저별로 전송 (중복 호출 방지)
        Map<Long, List<Notification>> notificationsGroupedByUser = notifications.stream()
                .collect(Collectors.groupingBy(Notification::getReceiverId));

        // 각 유저에게 알림을 한번에 전송
        notificationsGroupedByUser.forEach((receiverId, userNotifications) -> { // 맵의 엔트리(entry) 를 순회
            // 알림이 하나 이상 있을 때만 처리
            if (!userNotifications.isEmpty()) {
                kafkaProducer.sendMessage(userNotifications);  // 카프카 프로듀서로 알림 전송
                send(receiverId, userNotifications); // 알림 리스트 전송
            }
        });
    }

    /**
     * 사용자가 찜한 품목의 파티가 취소된 경우 해당 사용자에게 알림을 보냅니다.
     *
     * @param authUser 요청을 보낸 인증된 사용자 정보
     * @param partyId  취소된 파티의 ID
     */
    @Transactional
    @Override
    public void notifyUsersAboutPartyCancellation(AuthUser authUser, Long partyId) {
        User.fromAuthUser(authUser);
        Party party = validatePartyExists(partyId);

        // 메시지 구성
        String message = String.format("%s %s %s 품목의 파티가 취소되었습니다.",
                party.getMarketAddress(),
                party.getMarketName(),
                party.getItem().getCategory()
        );

        String notificationUrl = String.format("%s/parties", clientBasicUrl); // URL 생성

        // 알림 생성 및 저장
        List<Notification> notifications = party.getPartyMembers().stream()
                .map(partyMember -> Notification.builder()
                        .notificationType(NotificationType.PARTY_CREATE) // 알림 타입 설정
                        .content(message) // 알림 내용
                        .url(notificationUrl) // 알림 URL
                        .receiverId(partyMember.getUser().getId()) // 알림을 받을 유저 설정
                        .isRead(false) // 기본값 설정 (읽지 않음)
                        .build()
                )
                .toList();

        // 각 참가자에게 알림 전송
        notifications.forEach(notification -> {
            send(notification.getReceiverId(), List.of(notification));  // 각 사용자에게 알림을 전송
        });
    }

    /**
     * 참가 신청한 파티의 채팅창이 생성된 경우 해당 사용자에게 알림을 보냅니다.
     *
     * @param authUser         요청을 보낸 인증된 사용자 정보
     * @param chatRoomResponse 생성된 파티의 채팅창
     */
    @Transactional
    @Override
    public void notifyUsersAboutPartyChatCreation(AuthUser authUser, ChatRoomResponse chatRoomResponse) {
        User.fromAuthUser(authUser);
        Party party = validatePartyExists(chatRoomResponse.getPartyId());

        String notificationUrl = String.format("%s/chat/history/%d", clientBasicUrl, chatRoomResponse.getId()); // URL 생성


        // 메시지 구성
        String message = String.format("%s %s %s 품목의 채팅이 생성되었습니다.",
                party.getMarketAddress(),
                party.getMarketName(),
                party.getItem().getCategory()
        );

        // 알림 생성 및 저장
        List<Notification> notifications = party.getPartyMembers().stream()
                .map(partyMember -> Notification.builder()
                        .notificationType(NotificationType.CHAT_CREATE) // 알림 타입 설정
                        .content(message) // 알림 내용
                        .url(notificationUrl) // 알림 URL
                        .receiverId(partyMember.getUser().getId()) // 알림을 받을 유저 설정
                        .isRead(false) // 기본값 설정 (읽지 않음)
                        .build()
                )
                .toList();

        // 각 참가자에게 알림 전송
        notifications.forEach(notification -> {
            send(notification.getReceiverId(), List.of(notification));  // 각 사용자에게 알림을 전송
        });
    }

    /**
     * 사용자의 알림 목록을 조회합니다.
     *
     * @param authUser 요청을 보낸 인증된 사용자 정보
     * @return 사용자의 알림 목록을 포함한 NotificationListResponse
     */
    @Override
    public NotificationListResponse getNotifications(AuthUser authUser) {
        return NotificationListResponse.of(
                notificationRepository.findAllByReceiverIdOrderByCreatedAtDesc(authUser.getUserId()));
    }

    /**
     * 알림을 읽음 처리합니다.
     *
     * @param notificationId 읽음 처리할 알림 ID
     * @param authUser       요청을 보낸 인증된 사용자 정보
     * @throws CustomException 알림을 찾을 수 없거나 알림을 읽을 수 없는 경우 예외가 발생합니다.
     */
    @Override
    @Transactional
    public void readNotification(Long notificationId, AuthUser authUser) {
        verifyNotificationAccess(notificationId, authUser);
        Notification notification = notificationRepository.findById(notificationId)
                .orElseThrow(() -> new CustomException(ErrorCode.NOT_FOUND_NOTIFICATION));
        notification.read();
    }

    /**
     * 알림 삭제
     *
     * @param notificationId 삭제할 알림의 ID
     * @param authUser       요청을 보낸 인증된 사용자 정보
     * @throws CustomException 알림을 찾을 수 없거나 알림을 삭제할 권한이 없는 경우 예외가 발생합니다.
     */
    @Transactional
    @Override
    public void deleteNotification(Long notificationId, AuthUser authUser) {
        verifyNotificationAccess(notificationId, authUser);
        Notification notification = notificationRepository.findById(notificationId)
                .orElseThrow(() -> new CustomException(ErrorCode.NOT_FOUND_NOTIFICATION));
        notificationRepository.delete(notification);
    }

    /**
     * 특정 ID의 알림을 조회하고 권한을 검증합니다.
     *
     * @param notificationId 알림의 고유 ID
     * @param authUser       요청을 보낸 인증된 사용자 정보
     * @throws CustomException 해당 ID의 알림이 존재하지 않거나 접근 권한이 없을 경우 발생합니다.
     */
    @Override
    public void verifyNotificationAccess(Long notificationId, AuthUser authUser) {
        Notification notification = notificationRepository.findById(notificationId)
                .orElseThrow(() -> new CustomException(ErrorCode.NOT_FOUND_NOTIFICATION));

        if (!notification.getReceiverId().equals(authUser.getUserId())) {
            throw new CustomException(ErrorCode.UNAUTHORIZED_ACCESS);
        }
    }

    /**
     * 파티가 존재하는지 검증합니다.
     *
     * @param partyId 파티의 고유 ID
     * @return 파티가 존재하면 해당 파티 객체를 반환합니다.
     * @throws CustomException 파티가 존재하지 않으면 예외가 발생합니다.
     */
    @Override
    public Party validatePartyExists(Long partyId) {
        return partyRepository.findById(partyId)
                .orElseThrow(() -> new CustomException(ErrorCode.PARTY_NOT_FOUND));
    }

}
