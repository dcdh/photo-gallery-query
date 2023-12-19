package com.redhat.photogallery.query;

import com.redhat.photogallery.common.data.LikesAddedMessage;
import com.redhat.photogallery.common.data.PhotoCreatedMessage;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.eventbus.EventBus;
import io.vertx.mutiny.core.eventbus.MessageProducer;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertAll;

@QuarkusTest
public class QueryServiceTest {
    @Inject
    DataSource dataSource;

    @Inject
    EventBus eventBus;

    @BeforeEach
    public void setup() {
        try (final Connection connection = dataSource.getConnection();
             final PreparedStatement truncateStatement = connection.prepareStatement(
                     "TRUNCATE TABLE QueryItem")) {
            truncateStatement.execute();
        } catch (final SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void shouldConsumeCreatedPhoto() {
        // Given
        final MessageProducer<JsonObject> givenTopic = eventBus.publisher("photos");
        final PhotoCreatedMessage givenPhotoCreated = new PhotoCreatedMessage(1L, "Calinou", "animals");

        // When
        givenTopic.writeAndForget(JsonObject.mapFrom(givenPhotoCreated));

        // Then
        await().atMost(5, TimeUnit.SECONDS).until(() -> {
            try (final Connection connection = dataSource.getConnection();
                 final PreparedStatement countPreparedStatement = connection.prepareStatement(
                         "SELECT COUNT(*) AS count FROM QueryItem")) {
                final ResultSet countResultSet = countPreparedStatement.executeQuery();
                countResultSet.next();
                return Integer.valueOf(1).equals(countResultSet.getInt("count"));
            } catch (final SQLException e) {
                throw new RuntimeException(e);
            }
        });
        try (final Connection connection = dataSource.getConnection();
             final PreparedStatement selectItemsPreparedStatement = connection.prepareStatement(
                     "SELECT * FROM QueryItem")) {
            final ResultSet queryItemsResultSet = selectItemsPreparedStatement.executeQuery();
            queryItemsResultSet.next();
            assertAll(
                    () -> assertThat(queryItemsResultSet.getLong("id")).isEqualTo(1L),
                    () -> assertThat(queryItemsResultSet.getString("category")).isEqualTo("animals"),
                    () -> assertThat(queryItemsResultSet.getString("name")).isEqualTo("Calinou"),
                    () -> assertThat(queryItemsResultSet.getInt("likes")).isEqualTo(0)
            );
        } catch (final SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void shouldConsumeLikedPhoto() {
        // Given
        final MessageProducer<JsonObject> givenTopic = eventBus.publisher("likes");
        final LikesAddedMessage givenLikesAddedMessage = new LikesAddedMessage(1L, 10);

        // When
        givenTopic.writeAndForget(JsonObject.mapFrom(givenLikesAddedMessage));

        // Then
        await().atMost(5, TimeUnit.SECONDS).until(() -> {
            try (final Connection connection = dataSource.getConnection();
                 final PreparedStatement countPreparedStatement = connection.prepareStatement(
                         "SELECT COUNT(*) AS count FROM QueryItem")) {
                final ResultSet countResultSet = countPreparedStatement.executeQuery();
                countResultSet.next();
                return Integer.valueOf(1).equals(countResultSet.getInt("count"));
            } catch (final SQLException e) {
                throw new RuntimeException(e);
            }
        });
        try (final Connection connection = dataSource.getConnection();
             final PreparedStatement selectItemsPreparedStatement = connection.prepareStatement(
                     "SELECT * FROM QueryItem")) {

            final ResultSet queryItemsResultSet = selectItemsPreparedStatement.executeQuery();
            queryItemsResultSet.next();
            assertAll(
                    () -> assertThat(queryItemsResultSet.getLong("id")).isEqualTo(1L),
                    () -> assertThat(queryItemsResultSet.getString("category")).isNull(),
                    () -> assertThat(queryItemsResultSet.getString("name")).isNull(),
                    () -> assertThat(queryItemsResultSet.getInt("likes")).isEqualTo(10)
            );
        } catch (final SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void shouldReconcilePhotoWithLikes() throws InterruptedException {
        // Given
        final MessageProducer<JsonObject> givenPhotosTopic = eventBus.publisher("photos");
        final PhotoCreatedMessage givenPhotoCreated = new PhotoCreatedMessage(1L, "Calinou", "animals");
        final MessageProducer<JsonObject> givenLikedTopic = eventBus.publisher("likes");
        final LikesAddedMessage givenLikesAddedMessage = new LikesAddedMessage(1L, 10);

        // When
        givenPhotosTopic.writeAndForget(JsonObject.mapFrom(givenPhotoCreated));
        TimeUnit.SECONDS.sleep(1);// Ok this is necessary or the test will fail due to a Race condition :(
        // If I am not doing this it will 'sometime' fail because both consumers will want to create an QueryItem
        // I do not know how to fix it if using multiple instances ...
        // Two-way to fix it:
        // 1. use only one ConsumeEvent and use the header as a content type
        // 2. implement a retryer : do not know how to do it yet.
        givenLikedTopic.writeAndForget(JsonObject.mapFrom(givenLikesAddedMessage));

        // Then
        await().atMost(5, TimeUnit.SECONDS).until(() -> {
            try (final Connection connection = dataSource.getConnection();
                 final PreparedStatement countPreparedStatement = connection.prepareStatement(
                         "SELECT COUNT(*) AS count FROM QueryItem");
                 final PreparedStatement selectItemsPreparedStatement = connection.prepareStatement(
                         "SELECT * FROM QueryItem")) {
                final ResultSet countResultSet = countPreparedStatement.executeQuery();
                countResultSet.next();
                if (Integer.valueOf(0).equals(countResultSet.getInt("count"))) {
                    return false;
                }
                final ResultSet queryItemsResultSet = selectItemsPreparedStatement.executeQuery();
                queryItemsResultSet.next();
                return Integer.valueOf(1).equals(countResultSet.getInt("count"))
                       && queryItemsResultSet.getLong("id") != 0L
                       && queryItemsResultSet.getString("category") != null
                       && queryItemsResultSet.getString("name") != null
                       && queryItemsResultSet.getInt("likes") != 0;
            } catch (final SQLException e) {
                throw new RuntimeException(e);
            }
        });
        try (final Connection connection = dataSource.getConnection();
             final PreparedStatement selectItemsPreparedStatement = connection.prepareStatement(
                     "SELECT * FROM QueryItem")) {
            final ResultSet queryItemsResultSet = selectItemsPreparedStatement.executeQuery();
            queryItemsResultSet.next();
            assertAll(
                    () -> assertThat(queryItemsResultSet.getLong("id")).isEqualTo(1L),
                    () -> assertThat(queryItemsResultSet.getString("category")).isEqualTo("animals"),
                    () -> assertThat(queryItemsResultSet.getString("name")).isEqualTo("Calinou"),
                    () -> assertThat(queryItemsResultSet.getInt("likes")).isEqualTo(10)
            );
        } catch (final SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private record TestQueryItem(long id, String name, String category, int likes) {
    }

    @Test
    public void shouldReadCategoryOrderedByLikes() {
        // Given
        try (final Connection connection = dataSource.getConnection();
             final PreparedStatement insertItemsPreparedStatement = connection.prepareStatement(
                     "INSERT INTO QueryItem(id, name, category, likes) VALUES (?, ?, ?, ?)")) {
            List.of(
                            new TestQueryItem(1, "Odie", "animals", 5),
                            new TestQueryItem(2, "Garfield", "animals", 10),
                            new TestQueryItem(3, "Empire state building", "buildings", 0))
                    .forEach(testQueryItem -> {
                        try {
                            insertItemsPreparedStatement.setLong(1, testQueryItem.id());
                            insertItemsPreparedStatement.setString(2, testQueryItem.name());
                            insertItemsPreparedStatement.setString(3, testQueryItem.category());
                            insertItemsPreparedStatement.setInt(4, testQueryItem.likes());
                            insertItemsPreparedStatement.executeUpdate();
                        } catch (final SQLException e) {
                            throw new RuntimeException(e);
                        }
                    });
        } catch (final SQLException e) {
            throw new RuntimeException(e);
        }

        // When & Then
        given()
                .queryParam("category", "animals")
                .contentType(ContentType.JSON)
                .when()
                .get("/query")
                .then()
                .log().all()
                .statusCode(200)
                .log().all()
                .body("size()", is(2))
                .body("[0].id", is(2))
                .body("[0].name", is("Garfield"))
                .body("[0].category", is("animals"))
                .body("[0].likes", is(10))
                .body("[1].id", is(1))
                .body("[1].name", is("Odie"))
                .body("[1].category", is("animals"))
                .body("[1].likes", is(5));
    }

}
