package com.redhat.emergency.response;

import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.is;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

@QuarkusTest
public class MissionResourceTest {

    @Test
    public void testHelloEndpoint() {
        given()
          .when().get("/api/missions")
          .then()
             .statusCode(200)
             .body(is("hello"));
    }

}