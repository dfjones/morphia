package dev.morphia.test;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.bson.types.ObjectId;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.mongodb.client.result.UpdateResult;

import dev.morphia.UpdateOptions;
import dev.morphia.annotations.Entity;
import dev.morphia.annotations.Id;
import dev.morphia.query.Query;
import dev.morphia.query.filters.Filters;
import dev.morphia.query.updates.PullOperator;
import dev.morphia.query.updates.UpdateOperator;

public class TestUpdate extends TestBase {

  private EmbeddedDocument testEmbeddedDoc1 = new EmbeddedDocument(
      "foo1",
      "bar1",
      "baz1");

  private EmbeddedDocument testEmbeddedDoc2 = new EmbeddedDocument(
      "foo2",
      "bar2",
      "baz2");

  public TestUpdate() {
    super(buildConfig(MyDocument.class, EmbeddedDocument.class));
  }

  @Test
  public void testPullOp() {
    createTestDocuments();

    Query<MyDocument> query = getDs().find(MyDocument.class);
    EmbeddedDocument onlyField2 = new EmbeddedDocument(null, "bar1", null);

    UpdateOperator operator = new PullOperator("embeddedDocs", onlyField2);
    UpdateResult result = query.update(new UpdateOptions(), operator);

    Assert.assertEquals(result.getModifiedCount(), 1);

    Query<MyDocument> updatedQuery = getDs().find(MyDocument.class);
    MyDocument resultDoc = updatedQuery.first();
    Assert.assertFalse(resultDoc.embeddedDocs.contains(testEmbeddedDoc1));
  }

  @Test
  public void testPullOpWithFilter() {
    createTestDocuments();

    Query<MyDocument> query = getDs().find(MyDocument.class);
    // This is how we could write this more explicitly
    UpdateOperator operator = new PullOperator("embeddedDocs", Filters.eq("field2", "bar1"));
    UpdateResult result = query.update(new UpdateOptions(), operator);

    Assert.assertEquals(result.getModifiedCount(), 1);

    Query<MyDocument> updatedQuery = getDs().find(MyDocument.class);
    MyDocument resultDoc = updatedQuery.first();
    Assert.assertFalse(resultDoc.embeddedDocs.contains(testEmbeddedDoc1));
  }

  private void createTestDocuments() {
    MyDocument myDocument = new MyDocument();
    myDocument.embeddedDocs.addAll(List.of(testEmbeddedDoc1, testEmbeddedDoc2));
    getDs().save(myDocument);
  }

  @Entity("Documents")
  public static class MyDocument {

    @Id
    public ObjectId id;

    public List<EmbeddedDocument> embeddedDocs = new ArrayList<>();
  }

  @Entity
  public static class EmbeddedDocument {
    public String field1;
    public String field2;
    public String field3;

    public EmbeddedDocument(String field1, String field2, String field3) {
      this.field1 = field1;
      this.field2 = field2;
      this.field3 = field3;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      EmbeddedDocument that = (EmbeddedDocument) o;
      return Objects.equals(field1, that.field1) && Objects.equals(field2, that.field2)
          && Objects.equals(field3, that.field3);
    }

    @Override
    public int hashCode() {
      return Objects.hash(field1, field2, field3);
    }
  }

}
