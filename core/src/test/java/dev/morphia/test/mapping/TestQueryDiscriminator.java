package dev.morphia.test.mapping;

import static dev.morphia.query.filters.Filters.eq;

import org.bson.Document;
import org.testng.Assert;
import org.testng.annotations.Test;

import dev.morphia.Datastore;
import dev.morphia.annotations.Entity;
import dev.morphia.annotations.Id;
import dev.morphia.mapping.DiscriminatorFunction;
import dev.morphia.query.Query;
import dev.morphia.test.TestBase;

@SuppressWarnings("DataFlowIssue")
public class TestQueryDiscriminator extends TestBase {

    public TestQueryDiscriminator() {
        super(
            buildConfig(TestEntity.class)
                .discriminator(DiscriminatorFunction.className())
                .discriminatorKey("className")
                .enablePolymorphicQueries(false)
        );
    }

    @Test
    public void testQueryDoesNotContainDiscriminator() {
        Datastore datastore = getDs();
        Query<TestEntity> q = datastore.createQuery(TestEntity.class);
        q.filter(eq("something", "foo"));
        Document doc = q.toDocument();
        System.out.println(doc);
        Assert.assertFalse(doc.containsKey("className"), "The className field is not found in the query document");
    }

    interface TestInterface {

    }

    @Entity
    static class TestEntity implements TestInterface {
        @Id
        String id;

        String something;
    }


}
