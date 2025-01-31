package com.example.lastproject.domain.notification.service;

import com.example.lastproject.common.dto.AuthUser;
import com.example.lastproject.domain.chat.dto.ChatRoomResponse;
import com.example.lastproject.domain.notification.dto.NotificationListResponse;
import com.example.lastproject.domain.notification.dto.NotificationResponse;
import com.example.lastproject.domain.notification.entity.Notification;
import com.example.lastproject.domain.party.entity.Party;
import com.example.lastproject.domain.user.dto.NearbyBookmarkUserDto;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.List;

public interface NotificationService {

    // SSE 연결
    SseEmitter subscribe(AuthUser authUser, String lastEventId);

    // 비동기 방식으로 사용자의 SSE Emitter에 알림을 전송합니다.
    void sendNotifications(Long receiverId, List<Notification> notifications);

    // 알림 목록을 저장합니다.
    List<Notification> saveNotifications(List<Notification> notifications);

    // 주어진 알림 목록을 저장한 후, 해당 알림을 지정된 사용자에게 전송합니다.
    void send(Long receiverId, List<Notification> notifications);


    // 찜한 품목의 파티가 생성된 경우 알림
    void notifyUsersAboutPartyCreation(AuthUser authUser, Long partyId);

    // 찜한 품목의 파티가 취소된 경우 알림
    void notifyUsersAboutPartyCancellation(AuthUser authUser, Long partyId);

    // 참가 신청한 파티의 채팅창이 생성된 경우 알림
    void notifyUsersAboutPartyChatCreation(AuthUser authUser, ChatRoomResponse chatRoomResponse);



    // 사용자의 알림 목록을 조회합니다.
    NotificationListResponse getNotifications(AuthUser authUser);

    // 알림을 읽음 처리합니다.
    void readNotification(Long notificationId, AuthUser authUser);

    // 알림을 삭제합니다.
    void deleteNotification(Long notificationId, AuthUser authUser);

    // 특정 ID의 알림을 조회하고 권한을 검증합니다.
    void verifyNotificationAccess(Long notificationId, AuthUser authUser);

    // 파티가 존재하는지 검증합니다.
    Party validatePartyExists(Long partyId);

}
