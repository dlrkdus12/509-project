package com.example.lastproject.domain.user.dto;

import com.querydsl.core.annotations.QueryProjection;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

import java.math.BigDecimal;
import java.math.RoundingMode;

@Getter
@ToString
public class NearbyBookmarkUserDto {

    private final Long userId;
    private final String locationRange;

    @QueryProjection
    public NearbyBookmarkUserDto(Long userId, BigDecimal locationRange) {
        this.userId = userId;
        this.locationRange = locationRange.setScale(1, RoundingMode.HALF_UP) + "km";
    }

}
