package computerdatabase;

import io.gatling.javaapi.core.FeederBuilder;
import io.gatling.javaapi.core.ScenarioBuilder;
import io.gatling.javaapi.core.Simulation;
import io.gatling.javaapi.http.HttpProtocolBuilder;
import io.gatling.javaapi.http.SseMessageCheck;

import static io.gatling.javaapi.core.CoreDsl.*;
import static io.gatling.javaapi.http.HttpDsl.*;

public class SseSimulation extends Simulation {

    private final HttpProtocolBuilder httpProtocol = http
            .baseUrl("http://localhost:8080");

    SseMessageCheck sseCheck1 = sse.checkMessage("sse connection message");


    SseMessageCheck sseCheck2 = sse.checkMessage("party creation message");

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

    FeederBuilder<String> jwtFeeder = csv("jwtTokens.csv").circular();

    private final ScenarioBuilder scn = scenario("Notification Test")
            .feed(jwtFeeder)
            .exec(sse("SSE Connection")
                    .sseName("SSE Connection")
                    .get("/notifications/connect")
                    .header("Authorization", "Bearer #{jwtToken}")
                    .await(20)
                    .on(sseCheck1, sseCheck2));

    private final ScenarioBuilder party1 = scenario("http Test1")
            .feed(jwtFeeder)
            .exec(http("Party nowon Test")
                    .post("/parties")
                    .header("Authorization", "Bearer #{jwtToken}")
                    .body(StringBody(REGION_NOWON))
                    .asJson()
                    .check(status().is(201)));

    {
        setUp(
                scn.injectOpen(rampUsers(3000).during(10)),
                party1.injectOpen(rampUsers(3000).during(10))

        ).protocols(httpProtocol);
    }
}