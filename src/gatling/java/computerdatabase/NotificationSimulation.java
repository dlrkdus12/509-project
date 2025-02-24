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

    SseMessageCheck sseCheck1 = sse.checkMessage("sse connection message");
//            .check(bodyString().saveAs("responseBody1"));
//            .check(regex("id\":\"(.*?)\""));

    SseMessageCheck sseCheck2 = sse.checkMessage("party creation message");
//            .check(bodyString().saveAs("responseBody2"));

    SseMessageCheck sseCheck3 = sse.checkMessage("party creation message");
//            .check(bodyString().saveAs("responseBody3"));

    SseMessageCheck sseCheck4 = sse.checkMessage("party creation message");
//            .check(bodyString().saveAs("responseBody4"));

    // 파티 생성 요청 본문
    private static final String REGION_NOWON = """
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
    private static final String REGION_GANGNAM = """
                {
                    "marketName" : "이마트 강남점",
                    "marketAddress" : "서울 강남구",
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

    private static final String REGION_DONGJAK = """
                {
                    "marketName" : "이마트 동작점",
                    "marketAddress" : "서울 동작구",
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

    private final ScenarioBuilder sseScenario = scenario("Notification Test")
            .feed(jwtFeeder)
            .exec(sse("SSE Connection")
                    .sseName("SSE Connection")
                    .get("/notifications/connect")
                    .header("Authorization", "Bearer #{jwtToken}")
                    .await(100)
                    .on(sseCheck1, sseCheck2, sseCheck3, sseCheck4));
//            .exec(session -> {
//                String responseBody1 = session.getString("responseBody1");
//                String responseBody2 = session.getString("responseBody2");
//                String responseBody3 = session.getString("responseBody3");
//                String responseBody4 = session.getString("responseBody4");
//                System.out.println("🐝 Response Body: " + responseBody1);
//                System.out.println("🐝 Response Body: " + responseBody2);
//                System.out.println("🐝 Response Body: " + responseBody3);
//                System.out.println("🐝 Response Body: " + responseBody4);
//                return session;
//            });

    private final ScenarioBuilder party1 = scenario("Party Nowon")
            .feed(jwtFeeder)
            .exec(http("Event party nowon")
                    .post("/parties")
                    .header("Authorization", "Bearer #{jwtToken}")
                    .body(StringBody(REGION_NOWON))
                    .asJson()
                    .check(status().is(201)));

    private final ScenarioBuilder party2 = scenario("Party Dongjak")
            .feed(jwtFeeder)
            .exec(http("Event party dongjak")
                    .post("/parties")
                    .header("Authorization", "Bearer #{jwtToken}")
                    .body(StringBody(REGION_DONGJAK))
                    .asJson()
                    .check(status().is(201)));

    private final ScenarioBuilder party3 = scenario("Party Gangnam")
            .feed(jwtFeeder)
            .exec(http("Event party gangnam")
                    .post("/parties")
                    .header("Authorization", "Bearer #{jwtToken}")
                    .body(StringBody(REGION_GANGNAM))
                    .asJson()
                    .check(status().is(201)));

    {
        setUp(
                sseScenario.injectOpen(rampUsers(1000).during(130)),
                party1.injectOpen(rampUsers(1000).during(130)),
                party2.injectOpen(rampUsers(1000).during(130)),
                party3.injectOpen(rampUsers(1000).during(130))
        ).protocols(httpProtocol);
    }
}