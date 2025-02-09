package computerdatabase;

import io.gatling.javaapi.core.FeederBuilder;
import io.gatling.javaapi.core.ScenarioBuilder;
import io.gatling.javaapi.core.Simulation;
import io.gatling.javaapi.http.HttpProtocolBuilder;
import io.gatling.javaapi.http.SseMessageCheck;

import static io.gatling.javaapi.core.CoreDsl.*;
import static io.gatling.javaapi.http.HttpDsl.*;

public class NotificationSimulation extends Simulation {

    // HTTP 프로토콜 설정
    private final HttpProtocolBuilder httpProtocol = http
            .baseUrl("http://localhost:8080");

    SseMessageCheck sseCheck = sse.checkMessage("sse login message")
            .check(bodyString().saveAs("responseBody1"))
            .check(regex("id\":\"(.*?)\"").saveAs("lastEventId"));  // 이벤트의 id 추출

    SseMessageCheck sseCheck2 = sse.checkMessage("party creation message")
            .check(bodyString().saveAs("responseBody2"));

    // 파티 생성 요청 본문
    private static final String CREATE_PARTY_BODY = """
                {
                    "marketName" : "이마트 노원점",
                    "marketAddress" : "서울 노원구",
                    "latitude": "37.6973319258532",
                    "longitude": "127.047377408383",
                    "itemId" : 1,
                    "itemCount" : 2,
                    "itemUnit" : "kg",
                    "startTime" : "11-22 16:20",
                    "endTime" : "11-22 18:00",
                    "membersCount" : 3
                }
            """;

    FeederBuilder<String> jwtFeeder = csv("jwtTokens.csv").circular();

    private final ScenarioBuilder scn = scenario("SSE Latency Test")
            .feed(jwtFeeder)
            .exec(sse("SSE Connection")
                    .sseName("connection")
                    .get("/notifications/connect")
                    .header("Authorization", "Bearer #{jwtToken}")
                    .await(100).on(sseCheck, sseCheck2))
            .exec(session -> {
                // Session에서 responseBody 가져와서 로그에 출력
                String responseBody1 = session.getString("responseBody1");
                String responseBody2 = session.getString("responseBody2");
                System.out.println("🐝 Response Body: " + responseBody1);
                System.out.println("🐝 Response Body: " + responseBody2);
                return session;
            });

    private final ScenarioBuilder party = scenario("http Test")
            .feed(jwtFeeder)
            .exec(http("party")
                    .post("/parties")
                    .header("Authorization", "Bearer #{jwtToken}")
                    .body(StringBody(CREATE_PARTY_BODY))
                    .asJson()
                    .check(status().is(201)));

    {
        setUp(
                scn.injectOpen(rampUsers(5).during(10)), // 1명의 사용자가 테스트 실행
                party.injectOpen(rampUsers(5).during(10))
        ).protocols(httpProtocol);
    }
}