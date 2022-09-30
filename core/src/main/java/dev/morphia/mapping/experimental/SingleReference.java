package dev.morphia.mapping.experimental;

import com.mongodb.DBRef;
import com.mongodb.lang.Nullable;
import dev.morphia.Datastore;
import dev.morphia.annotations.internal.MorphiaInternal;
import dev.morphia.internal.EntityCache;
import dev.morphia.mapping.Mapper;
import dev.morphia.mapping.MappingException;
import dev.morphia.mapping.codec.pojo.EntityModel;
import dev.morphia.mapping.codec.pojo.PropertyModel;
import dev.morphia.mapping.lazy.proxy.ReferenceException;
import dev.morphia.query.Query;
import dev.morphia.sofia.Sofia;
import org.bson.Document;

import java.util.List;

import static dev.morphia.query.filters.Filters.eq;

/**
 * @param <T>
 * @morphia.internal
 */
@MorphiaInternal
@SuppressWarnings("unchecked")
@Deprecated(forRemoval = true, since = "2.3")
public class SingleReference<T> extends MorphiaReference<T> {
    private EntityModel entityModel;
    private Object id;
    private T value;

    /**
     * @param cache       the entity cache
     * @param datastore   the datastore to use
     * @param mapper      the mapper to use
     * @param entityModel the entity's mapped class
     * @param id          the ID value
     * @morphia.internal
     */
    @MorphiaInternal
    public SingleReference(EntityCache cache, Datastore datastore, Mapper mapper, EntityModel entityModel, Object id) {
        super(cache, datastore, mapper);
        this.entityModel = entityModel;
        this.id = id;
        if (entityModel.getType().isInstance(id)) {
            value = (T) id;
            PropertyModel idProperty = entityModel.getIdProperty();
            if (idProperty != null) {
                this.id = idProperty.getValue(value);
                resolve();
            } else {
                throw new MappingException(Sofia.noIdPropertyFound(entityModel.getType().getName()));
            }
        }
    }

    SingleReference(T value) {
        this.value = value;
    }

    /**
     * Decodes a document in to an entity
     *
     * @param datastore   the datastore
     * @param mapper      the mapper
     * @param mappedField the MappedField
     * @param paramType   the type of the underlying entity
     * @param document    the Document to decode
     * @return the entity
     */
    public static MorphiaReference<?> decode(Datastore datastore,
            Mapper mapper,
            PropertyModel mappedField,
            Class<?> paramType, Document document) {
        final EntityModel entityModel = mapper.getEntityModel(paramType);
        Object id = document.get(mappedField.getMappedName());

        return new SingleReference<>(EntityCache.get(), datastore, mapper, entityModel, id);
    }

    @Override
    public T get() {
        if (!isResolved() && value == null && id != null) {
            value = (T) getCache().get(getId());
            if (value == null) {
                value = (T) buildQuery().iterator().tryNext();
                if (value != null) {
                    getCache().set(getId(), value);
                }
            }
            if (value == null && !ignoreMissing()) {
                throw new ReferenceException(
                        Sofia.missingReferencedEntity(entityModel.getType().getSimpleName()));
            }
            resolve();
        }
        return value;
    }

    @Override
    public List<Object> getIds() {
        return List.of(getId());
    }

    @Override
    public Class<T> getType() {
        return (Class<T>) entityModel.getType();
    }

    @Override
    Object getId(Mapper mapper, EntityModel fieldClass) {
        if (id == null) {
            EntityModel entityModel = getEntityModel(mapper);
            if (entityModel != null && entityModel.getIdProperty() != null) {
                id = entityModel.getIdProperty().getValue(get());
                if (!entityModel.equals(fieldClass)) {
                    id = new DBRef(entityModel.getCollectionName(), id);
                }
            }
        }
        if (id == null) {
            throw new ReferenceException(Sofia.noIdForReference());
        }
        return id;
    }

    private Object getId() {
        return id instanceof DBRef ? ((DBRef) id).getId() : id;
    }

    Query<?> buildQuery() {
        final Query<?> query;
        if (id instanceof DBRef) {
            query = getDatastore().find(getDatastore()
                    .getMapper()
                    .getClassFromCollection(((DBRef) this.id).getCollectionName()));
        } else {
            query = getDatastore().find(entityModel.getType());
        }
        return query.filter(eq("_id", getId()));
    }

    @Nullable
    EntityModel getEntityModel(Mapper mapper) {
        if (entityModel == null) {
            T t = get();
            if (t != null) {
                entityModel = mapper.getEntityModel(t.getClass());
            }
        }

        return entityModel;
    }

}
