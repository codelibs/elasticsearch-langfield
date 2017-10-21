package org.codelibs.elasticsearch.langfield;

import static org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner.newConfigs;

import org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner;
import org.elasticsearch.action.DocWriteResponse.Result;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;

import junit.framework.TestCase;

public class LangFieldPluginTest extends TestCase {

    private ElasticsearchClusterRunner runner;

    private String clusterName;

    @Override
    protected void setUp() throws Exception {
        clusterName = "es-langfield-" + System.currentTimeMillis();
        // create runner instance
        runner = new ElasticsearchClusterRunner();
        // create ES nodes
        runner.onBuild(new ElasticsearchClusterRunner.Builder() {
            @Override
            public void build(final int number, final Builder settingsBuilder) {
                settingsBuilder.put("http.cors.enabled", true);
                settingsBuilder.put("http.cors.allow-origin", "*");
                settingsBuilder.putArray("discovery.zen.ping.unicast.hosts",
                        "localhost:9301-9310");
            }
        }).build(newConfigs().clusterName(clusterName).numOfNode(1).pluginTypes("org.codelibs.elasticsearch.langfield.LangFieldPlugin"));

        // wait for yellow status
        runner.ensureYellow();
    }

    @Override
    protected void tearDown() throws Exception {
        // close runner
        runner.close();
        // delete all files
        runner.clean();
    }

    public void test_basic() throws Exception {

        final String index = "test_index";
        final String type = "test_type";

        {
            // create an index
            runner.createIndex(index, (Settings) null);
            runner.ensureYellow(index);

            // create a mapping
            final XContentBuilder mappingBuilder = XContentFactory.jsonBuilder()//
                    .startObject()//
                    .startObject(type)//
                    .startObject("properties")//

                    // id
                    .startObject("id")//
                    .field("type", "keyword")//
                    .endObject()//

                    // message
                    .startObject("message")//
                    .field("type", "langstring")//
                    .endObject()//

                    .endObject()//
                    .endObject()//
                    .endObject();
            runner.createMapping(index, type, mappingBuilder);
        }

        if (!runner.indexExists(index)) {
            fail();
        }

        {
            String id = "none";
            final IndexResponse indexResponse1 = runner.insert(index, type, id,
                    "{\"id\":\"" + id + "\",\"message\":\"\",\"test\":\"\"}");
            assertEquals(Result.CREATED, indexResponse1.getResult());
        }
        {
            String id = "en";
            String message = "This is a pen.";
            final IndexResponse indexResponse1 = runner.insert(index, type, id,
                    "{\"id\":\"" + id + "\",\"message\":\"" + message + "\",\"test\":\"" + message + "\"}");
            assertEquals(Result.CREATED, indexResponse1.getResult());
        }
        {
            String id = "fr";
            String message = "C'est un stylo.";
            final IndexResponse indexResponse1 = runner.insert(index, type, id,
                    "{\"id\":\"" + id + "\",\"message\":\"" + message + "\"}");
            assertEquals(Result.CREATED, indexResponse1.getResult());
        }
        {
            String id = "ja";
            String message = "これはペンです。";
            final IndexResponse indexResponse1 = runner.insert(index, type, id,
                    "{\"id\":\"" + id + "\",\"message\":\"" + message + "\"}");
            assertEquals(Result.CREATED, indexResponse1.getResult());
        }
        {
            String id = "ko";
            String message = "이것은펜 이다 .";
            final IndexResponse indexResponse1 = runner.insert(index, type, id,
                    "{\"id\":\"" + id + "\",\"message\":\"" + message + "\"}");
            assertEquals(Result.CREATED, indexResponse1.getResult());
        }
        {
            String id = "zh-cn";
            String message = "这是一支笔。";
            final IndexResponse indexResponse1 = runner.insert(index, type, id,
                    "{\"id\":\"" + id + "\",\"message\":\"" + message + "\"}");
            assertEquals(Result.CREATED, indexResponse1.getResult());
        }
        {
            String id = "zh-tw";
            String message = "這是一支鋼筆。";
            final IndexResponse indexResponse1 = runner.insert(index, type, id,
                    "{\"id\":\"" + id + "\",\"message\":\"" + message + "\"}");
            assertEquals(Result.CREATED, indexResponse1.getResult());
        }

        runner.refresh();

        final Client client = runner.client();

        {
            SearchResponse response = client.prepareSearch(index).setTypes(type)
                    .setQuery(QueryBuilders.boolQuery()
                            .filter(QueryBuilders.existsQuery("message_en")))
                    .execute().actionGet();
            SearchHits searchHits = response.getHits();
            assertEquals(1, searchHits.getTotalHits());
        }
        {
            SearchResponse response = client.prepareSearch(index).setTypes(type)
                    .setQuery(QueryBuilders.boolQuery()
                            .filter(QueryBuilders.existsQuery("message_fr")))
                    .execute().actionGet();
            SearchHits searchHits = response.getHits();
            assertEquals(1, searchHits.getTotalHits());
        }
        {
            SearchResponse response = client.prepareSearch(index).setTypes(type)
                    .setQuery(QueryBuilders.boolQuery()
                            .filter(QueryBuilders.existsQuery("message_ja")))
                    .execute().actionGet();
            SearchHits searchHits = response.getHits();
            assertEquals(1, searchHits.getTotalHits());
        }
        {
            SearchResponse response = client.prepareSearch(index).setTypes(type)
                    .setQuery(QueryBuilders.boolQuery()
                            .filter(QueryBuilders.existsQuery("message_ko")))
                    .execute().actionGet();
            SearchHits searchHits = response.getHits();
            assertEquals(1, searchHits.getTotalHits());
        }
        {
            SearchResponse response = client.prepareSearch(index).setTypes(type)
                    .setQuery(QueryBuilders.boolQuery()
                            .filter(QueryBuilders.existsQuery("message_zh-cn")))
                    .execute().actionGet();
            SearchHits searchHits = response.getHits();
            assertEquals(1, searchHits.getTotalHits());
        }
        {
            SearchResponse response = client.prepareSearch(index).setTypes(type)
                    .setQuery(QueryBuilders.boolQuery()
                            .filter(QueryBuilders.existsQuery("message_zh-tw")))
                    .execute().actionGet();
            SearchHits searchHits = response.getHits();
            assertEquals(1, searchHits.getTotalHits());
        }

    }

    public void test_withLang() throws Exception {

        final String index = "test_index";
        final String type = "test_type";

        {
            // create an index
            runner.createIndex(index, (Settings) null);
            runner.ensureYellow(index);

            // create a mapping
            final XContentBuilder mappingBuilder = XContentFactory.jsonBuilder()//
                    .startObject()//
                    .startObject(type)//
                    .startObject("properties")//

                    // id
                    .startObject("id")//
                    .field("type", "keyword")//
                    .endObject()//

                    // message
                    .startObject("message")//
                    .field("type", "langstring")//
                    .field("lang_field", "lang")//
                    .endObject()//

                    // lang
                    .startObject("lang")//
                    .field("type", "keyword")//
                    .endObject()//

                    .endObject()//
                    .endObject()//
                    .endObject();
            runner.createMapping(index, type, mappingBuilder);
        }

        if (!runner.indexExists(index)) {
            fail();
        }

        {
            String id = "none";
            final IndexResponse indexResponse1 = runner.insert(index, type, id,
                    "{\"id\":\"" + id + "\",\"lang\":\"\",\"message\":\"\",\"test\":\"\"}");
            assertEquals(Result.CREATED, indexResponse1.getResult());
        }
        {
            String id = "en";
            String message = "This is a pen.";
            final IndexResponse indexResponse1 = runner.insert(index, type, id,
                    "{\"id\":\"" + id + "\",\"lang\":\"en\",\"message\":\"" + message + "\"}");
            assertEquals(Result.CREATED, indexResponse1.getResult());
        }
        {
            String id = "ja";
            String message = "This is a pen.";
            final IndexResponse indexResponse1 = runner.insert(index, type, id,
                    "{\"id\":\"" + id + "\",\"lang\":\"ja\",\"message\":\"" + message + "\"}");
            assertEquals(Result.CREATED, indexResponse1.getResult());
        }

        runner.refresh();

        final Client client = runner.client();

        {
            SearchResponse response = client.prepareSearch(index).setTypes(type)
                    .setQuery(QueryBuilders.boolQuery()
                            .filter(QueryBuilders.existsQuery("message_en")))
                    .execute().actionGet();
            SearchHits searchHits = response.getHits();
            assertEquals(1, searchHits.getTotalHits());
        }
        {
            SearchResponse response = client.prepareSearch(index).setTypes(type)
                    .setQuery(QueryBuilders.boolQuery()
                            .filter(QueryBuilders.existsQuery("message_ja")))
                    .execute().actionGet();
            SearchHits searchHits = response.getHits();
            assertEquals(1, searchHits.getTotalHits());
        }

    }

    public void test_withLangBaseName() throws Exception {

        final String index = "test_index";
        final String type = "test_type";

        {
            // create an index
            runner.createIndex(index, (Settings) null);
            runner.ensureYellow(index);

            // create a mapping
            final XContentBuilder mappingBuilder = XContentFactory.jsonBuilder()//
                    .startObject()//
                    .startObject(type)//
                    .startObject("properties")//

                    // id
                    .startObject("id")//
                    .field("type", "keyword")//
                    .endObject()//

                    // message
                    .startObject("message")//
                    .field("type", "langstring")//
                    .field("lang_base_name", "content")//
                    .field("lang_field", "lang")//
                    .field("index", false)//
                    .endObject()//

                    // content
                    .startObject("content")//
                    .field("type", "langstring")//
                    .field("lang_field", "lang")//
                    .endObject()//

                    // lang
                    .startObject("lang")//
                    .field("type", "keyword")//
                    .endObject()//

                    .endObject()//
                    .endObject()//
                    .endObject();
            runner.createMapping(index, type, mappingBuilder);
        }

        if (!runner.indexExists(index)) {
            fail();
        }

        {
            String id = "none";
            final IndexResponse indexResponse1 = runner.insert(index, type, id,
                    "{\"id\":\"" + id + "\",\"message\":\"\",\"test\":\"\"}");
            assertEquals(Result.CREATED, indexResponse1.getResult());
        }
        {
            String id = "default";
            String message = "This is a dog.";
            final IndexResponse indexResponse1 = runner.insert(index, type, id,
                    "{\"id\":\"" + id + "\",\"message\":\"" + message + "\"}");
            assertEquals(Result.CREATED, indexResponse1.getResult());
        }
        {
            String id = "en";
            String message = "This is a pineapple.";
            final IndexResponse indexResponse1 = runner.insert(index, type, id,
                    "{\"id\":\"" + id + "\",\"lang\":\"en\",\"message\":\"" + message + "\"}");
            assertEquals(Result.CREATED, indexResponse1.getResult());
        }
        {
            String id = "ja";
            String message = "This is a pen.";
            String content = "This is an apple.";
            final IndexResponse indexResponse1 = runner.insert(index, type, id,
                    "{\"id\":\"" + id + "\",\"lang\":\"ja\",\"message\":\"" + message + "\",\"content\":\"" + content + "\"}");
            assertEquals(Result.CREATED, indexResponse1.getResult());
        }

        runner.refresh();

        final Client client = runner.client();

        {
            SearchResponse response = client.prepareSearch(index).setTypes(type)
                    .setQuery(QueryBuilders.boolQuery()
                            .filter(QueryBuilders.existsQuery("message_en")))
                    .execute().actionGet();
            SearchHits searchHits = response.getHits();
            assertEquals(0, searchHits.getTotalHits());
        }
        {
            SearchResponse response = client.prepareSearch(index).setTypes(type)
                    .setQuery(QueryBuilders.boolQuery()
                            .filter(QueryBuilders.existsQuery("message_ja")))
                    .execute().actionGet();
            SearchHits searchHits = response.getHits();
            assertEquals(0, searchHits.getTotalHits());
        }
        {
            SearchResponse response = client.prepareSearch(index).setTypes(type)
                    .setQuery(QueryBuilders.boolQuery()
                            .filter(QueryBuilders.existsQuery("content_en")))
                    .execute().actionGet();
            SearchHits searchHits = response.getHits();
            assertEquals(2, searchHits.getTotalHits());
        }
        {
            SearchResponse response = client.prepareSearch(index).setTypes(type)
                    .setQuery(QueryBuilders.boolQuery()
                            .filter(QueryBuilders.existsQuery("content_ja")))
                    .execute().actionGet();
            SearchHits searchHits = response.getHits();
            assertEquals(1, searchHits.getTotalHits());
        }
        {
            SearchResponse response = client.prepareSearch(index).setTypes(type)
                    .setQuery(QueryBuilders.boolQuery()
                            .filter(QueryBuilders.matchQuery("content_ja", "pen")))
                    .execute().actionGet();
            SearchHits searchHits = response.getHits();
            assertEquals(1, searchHits.getTotalHits());
        }
        {
            SearchResponse response = client.prepareSearch(index).setTypes(type)
                    .setQuery(QueryBuilders.boolQuery()
                            .filter(QueryBuilders.matchQuery("content_ja", "apple")))
                    .execute().actionGet();
            SearchHits searchHits = response.getHits();
            assertEquals(1, searchHits.getTotalHits());
        }
        {
            SearchResponse response = client.prepareSearch(index).setTypes(type)
                    .setQuery(QueryBuilders.boolQuery()
                            .filter(QueryBuilders.matchQuery("content_ja", "pineapple")))
                    .execute().actionGet();
            SearchHits searchHits = response.getHits();
            assertEquals(0, searchHits.getTotalHits());
        }

    }
}
