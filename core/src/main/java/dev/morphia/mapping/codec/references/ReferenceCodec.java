package dev.morphia.mapping.codec.references;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import com.mongodb.DBRef;
import com.mongodb.lang.NonNull;
import com.mongodb.lang.Nullable;

import dev.morphia.MorphiaDatastore;
import dev.morphia.annotations.Reference;
import dev.morphia.annotations.internal.MorphiaInternal;
import dev.morphia.mapping.Mapper;
import dev.morphia.mapping.MappingException;
import dev.morphia.mapping.codec.Conversions;
import dev.morphia.mapping.codec.pojo.EntityModel;
import dev.morphia.mapping.codec.pojo.PropertyHandler;
import dev.morphia.mapping.codec.pojo.PropertyModel;
import dev.morphia.mapping.codec.pojo.TypeData;
import dev.morphia.mapping.codec.reader.DocumentReader;
import dev.morphia.mapping.codec.writer.DocumentWriter;
import dev.morphia.mapping.experimental.ListReference;
import dev.morphia.mapping.experimental.MapReference;
import dev.morphia.mapping.experimental.MorphiaReference;
import dev.morphia.mapping.experimental.SetReference;
import dev.morphia.mapping.experimental.SingleReference;
import dev.morphia.mapping.lazy.proxy.ReferenceException;
import dev.morphia.query.QueryException;
import dev.morphia.sofia.Sofia;

import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.Document;
import org.bson.codecs.BsonTypeClassMap;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecConfigurationException;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.TypeCache;
import net.bytebuddy.TypeCache.Sort;
import net.bytebuddy.description.ByteCodeElement;
import net.bytebuddy.description.modifier.Visibility;
import net.bytebuddy.dynamic.DynamicType.Builder;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy.Default;
import net.bytebuddy.implementation.InvocationHandlerAdapter;
import net.bytebuddy.matcher.ElementMatcher.Junction;
import net.bytebuddy.matcher.ElementMatchers;

import static dev.morphia.mapping.codec.CodecHelper.document;
import static java.lang.String.format;

/**
 * @hidden
 * @morphia.internal
 */
@SuppressWarnings({ "unchecked", "removal" })
@MorphiaInternal
public class ReferenceCodec extends BaseReferenceCodec<Object> implements PropertyHandler {
    private final Reference annotation;
    private final BsonTypeClassMap bsonTypeClassMap = new BsonTypeClassMap();
    private final Mapper mapper;

    /**
     * Name of instance field that holds the invocation handler of the proxy object.
     */
    private static final String FIELD_INVOCATION_HANDLER = "handler";
    /**
     * Type-cache for proxy classes generated w/ Byte Buddy.
     */
    private final TypeCache<TypeCache.SimpleKey> typeCache = new TypeCache.WithInlineExpunction<>(Sort.SOFT);
    private MorphiaDatastore datastore;

    /**
     * Creates a codec
     *
     * @param datastore
     * @param propertyModel the reference property
     */
    public ReferenceCodec(MorphiaDatastore datastore, PropertyModel propertyModel) {
        super(datastore, propertyModel);
        this.datastore = datastore;
        this.mapper = datastore.getMapper();
        annotation = getReferenceAnnotation(propertyModel);
    }

    /**
     * Encodes a value
     *
     * @param mapper
     * @param value  the value to encode
     * @param model  the mapped class of the field type
     * @return the encoded value
     * @hidden
     * @morphia.internal
     */
    @Nullable
    @MorphiaInternal
    public static Object encodeId(Mapper mapper, Object value, EntityModel model) {
        Object idValue;
        Class<?> type;
        idValue = mapper.getId(value);
        if (idValue == null) {
            return !mapper.isMappable(value.getClass()) ? value : null;
        }
        type = value.getClass();

        String valueCollectionName = mapper.getEntityModel(type).getCollectionName();
        String fieldCollectionName = model.getCollectionName();

        Reference annotation = model.getAnnotation(Reference.class);
        if (annotation != null && !annotation.idOnly()
                || valueCollectionName != null && !valueCollectionName.equals(fieldCollectionName)) {
            idValue = new DBRef(valueCollectionName, idValue);
        }
        return idValue;
    }

    /**
     * Decodes an ID value
     *
     * @param datastore      the Datastore to use
     * @param decode         the value to decode
     * @param decoderContext the decoder context
     * @return the decoded value
     */
    @NonNull
    public static Object processId(MorphiaDatastore datastore, Object decode, DecoderContext decoderContext) {
        Object id = decode;
        if (id instanceof Iterable) {
            Iterable<?> iterable = (Iterable<?>) id;
            List<Object> ids = new ArrayList<>();
            for (Object o : iterable) {
                ids.add(processId(datastore, o, decoderContext));
            }
            id = ids;
        } else if (id instanceof Document) {
            Document document = (Document) id;
            if (document.containsKey("$ref")) {
                id = processId(datastore, new DBRef(document.getString("$db"), document.getString("$ref"), document.get("$id")),
                        decoderContext);
            } else if (document.containsKey(datastore.getMapper().getConfig().discriminatorKey())) {
                try {
                    id = datastore.getCodecRegistry()
                            .get(datastore.getMapper().getClass(document))
                            .decode(new DocumentReader(document), decoderContext);
                } catch (CodecConfigurationException e) {
                    throw new MappingException(Sofia.cannotFindTypeInDocument(), e);
                }

            }
        } else if (id instanceof DBRef) {
            DBRef ref = (DBRef) id;
            Object refId = ref.getId();
            if (refId instanceof Document) {
                refId = datastore.getCodecRegistry()
                        .get(Object.class)
                        .decode(new DocumentReader((Document) refId), decoderContext);
            }
            id = new DBRef(ref.getDatabaseName(), ref.getCollectionName(), refId);
        }
        return id;
    }

    @Nullable
    @Override
    public Object decode(BsonReader reader, DecoderContext decoderContext) {
        Object decode = getDatastore().getCodecRegistry()
                .get(bsonTypeClassMap.get(reader.getCurrentBsonType()))
                .decode(reader, decoderContext);
        decode = processId(getDatastore(), decode, decoderContext);
        return fetch(decode);
    }

    private static TypeCache.SimpleKey getCacheKey(Class<?> type) {
        return new TypeCache.SimpleKey(type, Arrays.asList(type.getInterfaces()));
    }

    @Override
    @Nullable
    public Object encode(Object value) {
        try {
            DocumentWriter writer = new DocumentWriter(mapper.getConfig());
            document(writer, () -> {
                writer.writeName("ref");
                encode(writer, value, EncoderContext.builder().build());
            });
            return writer.getDocument().get("ref");
        } catch (ReferenceException e) {
            Reference refAnn = getPropertyModel().getAnnotation(Reference.class);
            if (refAnn != null && refAnn.ignoreMissing()) {
                return null;
            } else {
                throw e;
            }

        }
    }

    @Override
    public void encode(BsonWriter writer, Object instance, EncoderContext encoderContext) {
        Object idValue = collectIdValues(instance);

        if (idValue != null) {
            final Codec codec = getDatastore().getCodecRegistry().get(idValue.getClass());
            codec.encode(writer, idValue, encoderContext);
        } else if (getReferenceAnnotation(getPropertyModel()).ignoreMissing()) {
            writer.writeNull();
        } else {
            throw new ReferenceException(Sofia.noIdForReference());
        }
    }

    @Override
    public Class getEncoderClass() {
        TypeData type = getTypeData();
        List typeParameters = type.getTypeParameters();
        if (!typeParameters.isEmpty()) {
            type = (TypeData) typeParameters.get(typeParameters.size() - 1);
        }
        return type.getType();
    }

    @Nullable
    private Object collectIdValues(Object value) {
        if (value instanceof Collection) {
            return ((Collection<?>) value).stream()
                    .map(o -> collectIdValues(o))
                    .collect(Collectors.toCollection(ArrayList::new));
        } else if (value instanceof Map) {
            final Map<Object, Object> ids = new LinkedHashMap<>();
            Map<Object, Object> map = (Map<Object, Object>) value;
            for (Map.Entry<Object, Object> o : map.entrySet()) {
                ids.put(o.getKey().toString(), collectIdValues(o.getValue()));
            }
            return ids;
        } else if (value.getClass().isArray()) {
            return Arrays.stream((Object[]) value)
                    .map(o -> collectIdValues(o))
                    .collect(Collectors.toCollection(ArrayList::new));
        } else {
            return encodeId(value);
        }
    }

    private <T> T createProxy(MorphiaReference<?> reference) {
        ReferenceProxy referenceProxy = new ReferenceProxy(reference);
        PropertyModel propertyModel = getPropertyModel();
        try {
            Class<?> type = propertyModel.getType();
            // Get or create proxy class
            Class<T> proxyClass = (Class<T>) typeCache.findOrInsert(type.getClassLoader(), getCacheKey(type), this::makeProxy, typeCache);
            //... instantiate it
            final T proxy = proxyClass.getDeclaredConstructor().newInstance();
            // .. and set the invocation handler
            final Field field = proxyClass.getDeclaredField(FIELD_INVOCATION_HANDLER);
            field.setAccessible(true);
            field.set(proxy, referenceProxy);
            return proxy;
        } catch (ReflectiveOperationException | IllegalArgumentException e) {
            throw new MappingException(e.getMessage(), e);
        }
    }

    /**
     * Encodes a value
     *
     * @param value the value to encode
     * @return the encoded value
     */
    @Nullable
    private Object encodeId(Object value) {
        Object idValue;
        final String valueCollectionName;
        idValue = mapper.getId(value);
        if (idValue == null && !annotation.ignoreMissing()) {
            if (!mapper.isMappable(value.getClass())) {
                return value;
            }
            throw new QueryException("No ID value found on referenced entity.  Save referenced entities before defining references to"
                    + " them.");
        }
        valueCollectionName = mapper.getEntityModel(value.getClass()).getCollectionName();

        if (!annotation.idOnly()) {
            idValue = new DBRef(valueCollectionName, idValue);
        }
        return idValue;
    }

    private <T> Class<T> makeProxy() {
        PropertyModel propertyModel = getPropertyModel();
        Class<?> type = propertyModel.getType();
        Builder<?> builder = new ByteBuddy()
                .subclass(type)
                .implement(MorphiaProxy.class)
                .name(format("%s$%s$$ReferenceProxy", propertyModel.getEntityModel().getName(), propertyModel.getName()));

        Junction<ByteCodeElement> matcher = ElementMatchers.isDeclaredBy(type);
        if (!type.isInterface()) {
            type = type.getSuperclass();
            while (type != null && !type.equals(Object.class)) {
                matcher = matcher.or(ElementMatchers.isDeclaredBy(type));
                type = type.getSuperclass();
            }
        }

        return (Class<T>) builder
                .invokable(matcher.or(ElementMatchers.isDeclaredBy(MorphiaProxy.class)))
                .intercept(InvocationHandlerAdapter.toField(FIELD_INVOCATION_HANDLER))
                .defineField(FIELD_INVOCATION_HANDLER, InvocationHandler.class, Visibility.PRIVATE)
                .make()
                .load(Thread.currentThread().getContextClassLoader(), Default.WRAPPER)
                .getLoaded();
    }

    @Nullable
    private Object fetch(Object value) {
        MorphiaReference<?> reference;
        final Class<?> type = getPropertyModel().getType();
        if (List.class.isAssignableFrom(type)) {
            reference = readList((List<?>) value);
        } else if (Map.class.isAssignableFrom(type)) {
            reference = readMap((Map<Object, Object>) value);
        } else if (Set.class.isAssignableFrom(type)) {
            reference = readSet((List<?>) value);
        } else if (type.isArray()) {
            reference = readList((List<?>) value);
        } else if (value instanceof Document) {
            reference = readDocument((Document) value);
        } else {
            reference = readSingle(value);
        }
        reference.ignoreMissing(annotation.ignoreMissing());

        return !annotation.lazy() ? reference.get() : createProxy(reference);
    }

    private List<?> mapToEntitiesIfNecessary(List<?> value) {
        Codec<?> codec = getDatastore().getCodecRegistry().get(getEntityModelForField().getType());
        return value.stream()
                .filter(v -> v instanceof Document && ((Document) v).containsKey("_id"))
                .map(d -> codec.decode(new DocumentReader((Document) d), DecoderContext.builder().build()))
                .collect(Collectors.toList());
    }

    MorphiaReference<?> readDocument(Document value) {
        final Object id = getDatastore().getCodecRegistry().get(Object.class)
                .decode(new DocumentReader(value), DecoderContext.builder().build());
        return readSingle(id);
    }

    MorphiaReference<?> readList(List<?> value) {
        List<?> mapped = mapToEntitiesIfNecessary(value);
        return mapped.isEmpty()
                ? new ListReference<>(datastore, getEntityModelForField(), value)
                : new ListReference<>(datastore, mapped);
    }

    MorphiaReference<?> readMap(Map<Object, Object> value) {
        final Map<Object, Object> ids = new LinkedHashMap<>();
        Class<?> keyType = getTypeData().getTypeParameters().get(0).getType();
        for (Entry<Object, Object> entry : value.entrySet()) {
            ids.put(Conversions.convert(entry.getKey(), keyType), entry.getValue());
        }

        return new MapReference(datastore, ids, getEntityModelForField());
    }

    MorphiaReference<?> readSet(List<?> value) {
        List<?> mapped = mapToEntitiesIfNecessary(value);
        return mapped.isEmpty()
                ? new SetReference<>(datastore, getEntityModelForField(), value)
                : new SetReference<>(datastore, new LinkedHashSet<>(mapped));
    }

    MorphiaReference<?> readSingle(Object value) {
        return new SingleReference<>(datastore, getEntityModelForField(), value);
    }
}
