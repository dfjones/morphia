package dev.morphia.test.mapping;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.BsonWriter;
import org.bson.Document;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PropertyCodecRegistry;
import org.bson.codecs.pojo.TypeWithTypeParameters;
import org.bson.types.ObjectId;
import org.testng.Assert;
import org.testng.annotations.Test;

import dev.morphia.Datastore;
import dev.morphia.Morphia;
import dev.morphia.annotations.Entity;
import dev.morphia.annotations.Id;
import dev.morphia.mapping.DiscriminatorFunction;
import dev.morphia.mapping.MapperOptions;
import dev.morphia.mapping.NamingStrategy;
import dev.morphia.mapping.codec.MorphiaPropertyCodecProvider;
import dev.morphia.test.TestBase;

public class TestEnumCustomCodec extends TestBase {

  @Test
  public void mapSingleCustomEnum() {
    Datastore ds = setupCustomMapping();

    ModelWithEnum e1 = new ModelWithEnum();
    e1.customEnum = MyCustomEnum.SECOND;

    ds.save(e1);

    ModelWithEnum found = ds.find(ModelWithEnum.class).first();
    Assert.assertNotNull(found);
    Assert.assertEquals(found.customEnum, e1.customEnum);

    String collectionName = ds.getCollection(ModelWithEnum.class).getNamespace().getCollectionName();
    Document doc = ds.getDatabase().getCollection(collectionName).find().first();
    Assert.assertEquals(doc.get("customEnum"), e1.customEnum.value);
  }

  @Test
  public void mapListCustomEnum() {
    Datastore ds = setupCustomMapping();

    ModelWithEnumList e1 = new ModelWithEnumList();
    e1.listCustomEnum = List.of(MyCustomEnum.FIRST, MyCustomEnum.SECOND);

    ds.save(e1);

    ModelWithEnumList found = ds.find(ModelWithEnumList.class).first();
    Assert.assertNotNull(found);
    Assert.assertEquals(found.listCustomEnum.get(0), e1.listCustomEnum.get(0));
    Assert.assertEquals(found.listCustomEnum.get(1), e1.listCustomEnum.get(1));

    String collectionName = ds.getCollection(ModelWithEnumList.class).getNamespace().getCollectionName();
    Document doc = ds.getDatabase().getCollection(collectionName).find().first();
    List<String> list = doc.getList("listCustomEnum", String.class);
    Assert.assertEquals(list.get(0), e1.listCustomEnum.get(0).value());
    Assert.assertEquals(list.get(1), e1.listCustomEnum.get(1).value());
  }

  @Test
  public void mapListStandardEnum() {
    Datastore ds = setupCustomMapping();

    ModelWithStandardEnumList e1 = new ModelWithStandardEnumList();
    e1.listStandardEnum = List.of(StandardEnum.A, StandardEnum.B);

    ds.save(e1);

    ModelWithStandardEnumList found = ds.find(ModelWithStandardEnumList.class).first();
    Assert.assertNotNull(found);
    Assert.assertEquals(found.listStandardEnum.get(0), e1.listStandardEnum.get(0));
    Assert.assertEquals(found.listStandardEnum.get(1), e1.listStandardEnum.get(1));

    String collectionName = ds.getCollection(ModelWithStandardEnumList.class).getNamespace().getCollectionName();
    Document doc = ds.getDatabase().getCollection(collectionName).find().first();
    List<String> list = doc.getList("listStandardEnum", String.class);
    Assert.assertEquals(list.get(0), e1.listStandardEnum.get(0).name());
    Assert.assertEquals(list.get(1), e1.listStandardEnum.get(1).name());
  }

  @SuppressWarnings("deprecated")
  private Datastore setupCustomMapping() {
    MapperOptions mapperOptions = MapperOptions.builder()
        .discriminatorKey("className")
        .discriminator(DiscriminatorFunction.className())
        .collectionNaming(NamingStrategy.identity())
        .propertyNaming(NamingStrategy.identity())
        .codecProvider(new CustomCodecProvider())
        .build();

    Datastore ds = Morphia.createDatastore(getMongoClient(), TEST_DB_NAME, mapperOptions);
    ds.getMapper().map(ModelWithEnum.class, ModelWithEnumList.class, ModelWithStandardEnumList.class);
    return ds;
  }


  private static class CustomCodecProvider implements CodecProvider {

    @Override
    @SuppressWarnings("unchecked")
    public <T> Codec<T> get(Class<T> type, CodecRegistry codecRegistry) {
      if (CustomEnumValue.class.isAssignableFrom(type) && Enum.class.isAssignableFrom(type)) {
        try {
          Method value = type.getMethod("value");
          Class<?> returnType = value.getReturnType();

          if (returnType == String.class) {
            return new StringCustomEnumValueCodec(type);
          }
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }

      return null;
    }
  }

  public static class StringCustomEnumValueCodec<T extends Enum<T> & CustomEnumValue<String>> implements Codec<T> {

    private final Class<T> type;
    private final Map<String, T> valueMap;

    public StringCustomEnumValueCodec(Class<T> type) {
      this.type = type;
      this.valueMap = CustomEnumValueMapper.map(type);
    }

    @Override
    public T decode(BsonReader bsonReader, DecoderContext decoderContext) {
      if (bsonReader.getCurrentBsonType() == BsonType.STRING) {
        return valueMap.get(bsonReader.readString());
      }

      throw new IllegalArgumentException("unexpected type");
    }

    @Override
    public void encode(BsonWriter bsonWriter, T t, EncoderContext encoderContext) {
      bsonWriter.writeString(t.value());
    }

    @Override
    public Class<T> getEncoderClass() {
      return type;
    }
  }

  @Entity
  private static class ModelWithEnum {
    @Id
    ObjectId id;

    MyCustomEnum customEnum;
  }

  @Entity
  private static class ModelWithEnumList {
    @Id
    ObjectId id;

    List<MyCustomEnum> listCustomEnum;
  }

  @Entity
  private static class ModelWithStandardEnumList {

    @Id
    ObjectId id;

    List<StandardEnum> listStandardEnum;
  }


  private static class CustomEnumValueMapper {
    private static <T extends Enum<T> & CustomEnumValue<V>, V> Map<V, T> map(Class<T> type) {
      final T[] values = type.getEnumConstants();
      Map<V, T> valMap = new HashMap<>();
      for (T t : values) {
        valMap.put(t.value(), t);
      }

      return valMap;
    }
  }

  public interface CustomEnumValue<T> {
    T value();
  }

  public enum MyCustomEnum implements CustomEnumValue<String> {

    FIRST("firstCustomValue"),
    SECOND("secondCustomValue");

    private final String value;

    MyCustomEnum(String value) {
      this.value = value;
    }


    @Override
    public String value() {
      return this.value;
    }
  }

  public enum StandardEnum {
    A,
    B;
  }

}
