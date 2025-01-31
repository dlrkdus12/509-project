package com.example.lastproject.domain.notification.dto;

import com.example.lastproject.domain.notification.entity.Notification;
import com.example.lastproject.domain.notification.entity.NotificationType;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Builder;
import lombok.Getter;

import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;

@Getter
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL) // null 필드 제외
@JsonIgnoreProperties(ignoreUnknown = true) // 알 수 없는 필드 무시
public class NotificationResponse {

    private final Long id;
    private final String content;
    private final NotificationType type;
    private final String url;
    private final Boolean isRead;
    private final LocalDateTime createdAt;

    // 단일 알림을 처리하는 메서드
    public static NotificationResponse of(Notification notification) {
        return NotificationResponse.builder()
                .id(notification.getId())
                .content(notification.getContent())
                .type(notification.getNotificationType())
                .url(notification.getUrl())
                .isRead(notification.isRead())
                .createdAt(notification.getCreatedAt())
                .build();
    }

    // 여러 알림을 처리하는 메서드
    public static List<NotificationResponse> of(List<Notification> notifications) {
        return notifications.stream()
                .map(NotificationResponse::of)
                .collect(Collectors.toList());
    }

    // 문자열 메시지만 처리하는 메서드 추가
    public static NotificationResponse of(String message) {
        return NotificationResponse.builder()
                .content(message)
                .build();
    }

}
