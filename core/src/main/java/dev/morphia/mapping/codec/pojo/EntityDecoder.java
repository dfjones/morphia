package dev.morphia.mapping.codec.pojo;

import com.mongodb.lang.Nullable;

import dev.morphia.annotations.internal.MorphiaInternal;
import dev.morphia.mapping.DiscriminatorLookup;
import dev.morphia.mapping.codec.MorphiaInstanceCreator;

import org.bson.BsonInvalidOperationException;
import org.bson.BsonReader;
import org.bson.BsonReaderMark;
import org.bson.BsonType;
import org.bson.codecs.Codec;
import org.bson.codecs.Decoder;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.configuration.CodecConfigurationException;
import org.bson.codecs.configuration.CodecRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static dev.morphia.mapping.codec.Conversions.convert;
import static java.lang.String.format;

/**
 * @param <T> the entity type
 * @hidden
 * @morphia.internal
 * @since 2.0
 */
@MorphiaInternal
public class EntityDecoder<T> implements Decoder<T> {
    private static final Logger LOG = LoggerFactory.getLogger(EntityDecoder.class);

    private final MorphiaCodec<T> morphiaCodec;
    private final EntityModel classModel;

    protected EntityDecoder(MorphiaCodec<T> morphiaCodec) {
        this.morphiaCodec = morphiaCodec;
        classModel = morphiaCodec.getEntityModel();
    }

    @Override
    @SuppressWarnings("unchecked")
    public T decode(BsonReader reader, DecoderContext decoderContext) {
        T entity;
        if (decoderContext.hasCheckedDiscriminator()) {
            LOG.debug(format("Decoding document using codec for %s'", morphiaCodec.getEntityModel().getType().getName()));
            MorphiaInstanceCreator instanceCreator = getInstanceCreator();
            decodeProperties(reader, decoderContext, instanceCreator, classModel);
            return (T) instanceCreator.getInstance();
        } else {
            entity = getCodecFromDocument(reader, classModel.useDiscriminator(), classModel.getDiscriminatorKey(),
                    morphiaCodec.getRegistry(), morphiaCodec.getDiscriminatorLookup(), morphiaCodec)
                    .decode(reader, DecoderContext.builder().checkedDiscriminator(true).build());
        }

        return entity;
    }

    protected void decodeModel(BsonReader reader, DecoderContext decoderContext,
            MorphiaInstanceCreator instanceCreator, @Nullable PropertyModel model) {

        if (model != null) {
            final BsonReaderMark mark = reader.getMark();
            try {
                if (reader.getCurrentBsonType() == BsonType.NULL) {
                    reader.readNull();
                } else {
                    Object value = decoderContext.decodeWithChildContext(model.getCodec(), reader);
                    instanceCreator.set(value, model);
                }
            } catch (BsonInvalidOperationException e) {
                mark.reset();
                final Object value = morphiaCodec.getRegistry().get(Object.class).decode(reader, decoderContext);
                instanceCreator.set(convert(value, model.getTypeData().getType()), model);
            }
        } else {
            reader.skipValue();
        }
    }

    protected void decodeProperties(BsonReader reader, DecoderContext decoderContext,
            MorphiaInstanceCreator instanceCreator, EntityModel classModel) {
        reader.readStartDocument();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            String name = reader.readName();
            if (classModel.useDiscriminator() && classModel.getDiscriminatorKey().equals(name)) {
                reader.readString();
            } else {
                decodeModel(reader, decoderContext, instanceCreator, classModel.getProperty(name));
            }
        }
        reader.readEndDocument();
    }

    protected Codec<T> getCodecFromDocument(BsonReader reader, boolean useDiscriminator, String discriminatorKey,
            CodecRegistry registry, DiscriminatorLookup discriminatorLookup,
            Codec<T> defaultCodec) {
        Codec<T> codec = null;
        if (useDiscriminator) {
            BsonReaderMark mark = reader.getMark();
            try {
                reader.readStartDocument();
                while (codec == null && reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                    if (discriminatorKey.equals(reader.readName())) {
                        codec = (Codec<T>) registry.get(discriminatorLookup.lookup(reader.readString()));
                    } else {
                        reader.skipValue();
                    }
                }
            } catch (Exception e) {
                throw new CodecConfigurationException(format("Failed to decode '%s'. Decoding errored with: %s",
                        morphiaCodec.getEntityModel().getName(), e.getMessage()), e);
            } finally {
                mark.reset();
            }
        }
        return codec != null ? codec : defaultCodec;
    }

    protected MorphiaInstanceCreator getInstanceCreator() {
        return classModel.getInstanceCreator();
    }

    protected MorphiaCodec<T> getMorphiaCodec() {
        return morphiaCodec;
    }
}
