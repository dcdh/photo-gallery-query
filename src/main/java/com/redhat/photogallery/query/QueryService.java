package com.redhat.photogallery.query;

import com.redhat.photogallery.common.Constants;
import com.redhat.photogallery.common.data.LikesAddedMessage;
import com.redhat.photogallery.common.data.PhotoCreatedMessage;
import io.quarkus.vertx.ConsumeEvent;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.eventbus.Message;
import jakarta.persistence.EntityManager;
import jakarta.persistence.TypedQuery;
import jakarta.transaction.SystemException;
import jakarta.transaction.UserTransaction;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.GenericEntity;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;

@Path("/query")
public class QueryService {

    private static final Logger LOG = LoggerFactory.getLogger(QueryService.class);

    private final UserTransaction userTransaction;
    private final EntityManager entityManager;
    private final PersistenceRaceConditionRetryStrategy persistenceRaceConditionRetryStrategy;

    public QueryService(final UserTransaction userTransaction,
                        final EntityManager entityManager,
                        final PersistenceRaceConditionRetryStrategy persistenceRaceConditionRetryStrategy) {
        this.userTransaction = Objects.requireNonNull(userTransaction);
        this.entityManager = Objects.requireNonNull(entityManager);
        this.persistenceRaceConditionRetryStrategy = Objects.requireNonNull(persistenceRaceConditionRetryStrategy);
    }

    @ConsumeEvent(value = Constants.PHOTOS_TOPIC_NAME, blocking = true, ordered = true)
    public void onNextPhotoCreated(final Message<JsonObject> photoObject) {
        persistenceRaceConditionRetryStrategy.execute(() -> {
            try {
                userTransaction.begin();
                final PhotoCreatedMessage message = photoObject.body().mapTo(PhotoCreatedMessage.class);
                QueryItem savedItem = entityManager.find(QueryItem.class, message.getId());
                if (savedItem == null) {
                    savedItem = new QueryItem();
                    savedItem.id = message.getId();
                    savedItem.persist();
                }
                savedItem.name = message.getName();
                savedItem.category = message.getCategory();
                LOG.info("Updated in data store {}", savedItem);
                userTransaction.commit();
            } catch (final Exception exception) {
                try {
                    userTransaction.rollback();
                } catch (final SystemException systemException) {
                    throw new FailedToPersistException(systemException);
                }
                throw new FailedToPersistException(exception);
            }
        });
    }

    @ConsumeEvent(value = Constants.LIKES_TOPIC_NAME, blocking = true, ordered = true)
    public void onNextLikesAdded(final Message<JsonObject> likesObject) {
        persistenceRaceConditionRetryStrategy.execute(() -> {
            try {
                userTransaction.begin();
                final LikesAddedMessage message = likesObject.body().mapTo(LikesAddedMessage.class);
                QueryItem savedItem = entityManager.find(QueryItem.class, message.getId());
                if (savedItem == null) {
                    savedItem = new QueryItem();
                    savedItem.id = message.getId();
                    savedItem.persist();
                }
                savedItem.likes = message.getLikes();
                LOG.info("Updated in data store {}", savedItem);
                userTransaction.commit();
            } catch (final Exception exception) {
                try {
                    userTransaction.rollback();
                } catch (final SystemException systemException) {
                    throw new FailedToPersistException(systemException);
                }
                throw new FailedToPersistException(exception);
            }
        });
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response readCategoryOrderedByLikes(@QueryParam("category") final String category) {
        final TypedQuery<QueryItem> query = entityManager.createQuery("FROM QueryItem WHERE category =?1 ORDER BY likes DESC", QueryItem.class);
        query.setParameter(1, category);
        final List<QueryItem> items = query.getResultList();
        LOG.info("Returned {} items in category {}", items.size(), category);
        return Response.ok(new GenericEntity<>(items) {
        }).build();
    }

}