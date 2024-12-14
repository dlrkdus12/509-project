package com.example.lastproject.domain.party.repository;

import com.example.lastproject.domain.user.dto.NearbyBookmarkUserDto;
import com.example.lastproject.domain.user.dto.QNearbyBookmarkUserDto;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.core.types.dsl.NumberExpression;
import com.querydsl.jpa.impl.JPAQueryFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.util.List;

import static com.example.lastproject.domain.likeitem.entity.QLikeItem.likeItem;
import static com.example.lastproject.domain.user.entity.QUser.user;

@Slf4j
@Repository
@RequiredArgsConstructor
public class PartyQueryRepositoryImpl implements PartyQueryRepository {

    private final JPAQueryFactory queryFactory;

    // 파티 생성시 등록한 마트의 반경 10 km 내에 유저들 중 즐겨찾기 품목과 파티의 장 볼 품목이 동일한 유저들의 ID와 생성된 파티와의 거리를 반환한 dto 리스트 반환
    public List<NearbyBookmarkUserDto> getUserIdWithDistanceNearbyParty(BigDecimal latitude, BigDecimal longitude, long itemId) {

        // 거리 계산을 위한 하버사인 공식 (NumberTemplate 사용)
        NumberExpression<BigDecimal> distance = Expressions.numberTemplate(BigDecimal.class,
                "6371 * acos(cos(radians({0})) * cos(radians({1})) * cos(radians({2}) - radians({3})) + sin(radians({0})) * sin(radians({1})))",
                latitude, user.latitude, user.longitude, longitude);

        List<NearbyBookmarkUserDto> results = queryFactory
                .select(new QNearbyBookmarkUserDto(user.id, distance))
                .from(user)
                .leftJoin(likeItem).on(likeItem.user.id.eq(user.id))  // user와 likeItem을 연결하는 조건 추가
                .where(distance.loe(10)) // 10KM 이하의 거리 필터
                .where(likeItem.item.id.eq(itemId)) // 파티의 품목과 유저의 즐겨찾기 품목이 같은지 필터
                .fetch();
        return results;
    }

}
