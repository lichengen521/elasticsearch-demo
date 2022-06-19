package com.itheima.elasticsearchdemo;

import com.alibaba.fastjson.JSON;
import com.itheima.elasticsearchdemo.domain.Goods;
import com.itheima.elasticsearchdemo.domain.Person;
import com.itheima.elasticsearchdemo.mapper.GoodsMapper;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.*;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SpringBootTest
class ElasticsearchDemoApplicationTests {

    @Autowired
    private RestHighLevelClient client;

    @Autowired
    private GoodsMapper goodsMapper;

    @Test
    void contextLoads() {
       /* //1.创建ES客户端对象
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(
           new HttpHost(
                   "192.168.149.135",
                   9200,
                   "http"
           )
        ));*/

        System.out.println(client);


    }


    /**
     * 添加索引
     */
    @Test
    public void addIndex() throws IOException {
        //1.使用client获取操作索引的对象
        IndicesClient indices = client.indices();

        //2.具体操作，获取返回值
        CreateIndexRequest createRequlest = new CreateIndexRequest("itheima");
        CreateIndexResponse createIndexResponse = indices.create(createRequlest, RequestOptions.DEFAULT);

        //3.根据返回值判断结果

        if (createIndexResponse.isAcknowledged()) {
            System.out.println(true);
        } else {
            System.out.println(false);
        }


    }


    /**
     * 添加索引 并配置mapping关系
     */
    @Test
    public void addIndexAndMapping() throws IOException {
        //1.使用client获取操作索引的对象
        IndicesClient indicesClient = client.indices();
        //2.具体操作，获取返回值
        CreateIndexRequest createRequest = new CreateIndexRequest("itcast");
        //2.1 设置mappings
        String mapping = "{\n" +
                "      \"properties\" : {\n" +
                "        \"address\" : {\n" +
                "          \"type\" : \"text\",\n" +
                "          \"analyzer\" : \"ik_max_word\"\n" +
                "        },\n" +
                "        \"age\" : {\n" +
                "          \"type\" : \"long\"\n" +
                "        },\n" +
                "        \"name\" : {\n" +
                "          \"type\" : \"keyword\"\n" +
                "        }\n" +
                "      }\n" +
                "    }";
        createRequest.mapping(mapping, XContentType.JSON);


        CreateIndexResponse response = indicesClient.create(createRequest, RequestOptions.DEFAULT);

        //3.根据返回值判断结果
        System.out.println(response.isAcknowledged());


    }


    /**
     * 查询索引
     */
    @Test
    public void queryIndex() throws IOException {
        //1.使用client获取操作索引的对象
        IndicesClient indices = client.indices();

        GetIndexRequest GetIndexRequest = new GetIndexRequest("itcast");
        GetIndexResponse getIndexResponse = indices.get(GetIndexRequest, RequestOptions.DEFAULT);

        Map<String, MappingMetaData> mappings = getIndexResponse.getMappings();

        for (String key : mappings.keySet()) {
            System.out.println(key + ":" + mappings.get(key).getSourceAsMap());
        }

    }


    /**
     * 删除索引
     */
    @Test
    public void deleteIndex() throws IOException {
        //1.使用client获取操作索引的对象
        IndicesClient indices = client.indices();

        DeleteIndexRequest deleteRequest = new DeleteIndexRequest("itheima");
        AcknowledgedResponse response = indices.delete(deleteRequest, RequestOptions.DEFAULT);

        System.out.println(response.isAcknowledged());

    }

    /**
     * 判断索引是否存在
     */
    @Test
    public void existIndex() throws IOException {
        IndicesClient indices = client.indices();

        GetIndexRequest getRequest = new GetIndexRequest("itcast");
        boolean exists = indices.exists(getRequest, RequestOptions.DEFAULT);

        System.out.println(exists);

    }


    /**
     * 添加文档,使用map作为数据
     */
    @Test
    public void addDoc() throws IOException {
        //数据对象，map
        Map data = new HashMap();
        data.put("address", "北京昌平");
        data.put("name", "大胖");
        data.put("age", 20);


        //1.获取操作文档的对象
        IndexRequest request = new IndexRequest("itcast").id("1").source(data);
        //添加数据，获取结果
        IndexResponse response = client.index(request, RequestOptions.DEFAULT);

        //打印响应结果
        System.out.println(response.getId());


    }


    /**
     * 添加文档,使用对象作为数据
     */
    @Test
    public void addDoc2() throws IOException {
        //数据对象，javaObject
        Person p = new Person();
        p.setId("2");
        p.setName("小胖2222");
        p.setAge(30);
        p.setAddress("陕西西安");

        //将对象转为json
        String data = JSON.toJSONString(p);

        //1.获取操作文档的对象
        IndexRequest request = new IndexRequest("itcast").id(p.getId()).source(data, XContentType.JSON);
        //添加数据，获取结果
        IndexResponse response = client.index(request, RequestOptions.DEFAULT);

        //打印响应结果
        System.out.println(response.getId());


    }


    /**
     * 修改文档：添加文档时，如果id存在则修改，id不存在则添加
     */
    @Test
    public void updateDoc() throws IOException {
        //数据对象，javaObject
        Person p = new Person();
        p.setId("3");
        p.setName("小胖2222");
        p.setAge(30);
        p.setAddress("陕西西安");

        //将对象转为json
        String data = JSON.toJSONString(p);

        //1.获取操作文档的对象
        IndexRequest request = new IndexRequest("itcast").id(p.getId()).source(data, XContentType.JSON);
        //添加数据，获取结果
        IndexResponse response = client.index(request, RequestOptions.DEFAULT);

        //打印响应结果
        System.out.println(response.getId());

    }


    /**
     * 根据id查询文档
     */
    @Test
    public void findDocById() throws IOException {

        GetRequest getReqeust = new GetRequest("itcast", "1");
        //getReqeust.id("1");
        GetResponse response = client.get(getReqeust, RequestOptions.DEFAULT);
        //获取数据对应的json
        System.out.println(response.getSourceAsString());


    }


    /**
     * 根据id删除文档
     */
    @Test
    public void delDoc() throws IOException {


        DeleteRequest deleteRequest = new DeleteRequest("itcast", "1");
        DeleteResponse response = client.delete(deleteRequest, RequestOptions.DEFAULT);
        System.out.println(response.getId());

    }

    /**
     * java api 批量操作es脚本 bulk
     */
    @Test
    public void testBulk() throws IOException {
        //创建BulkRequest 对象 整合所有操作
        BulkRequest bulkRequest = new BulkRequest();

        //添加操作
        /*
        #bulk操作 即 批量操作
            #1、删除操作
            #2、添加操作
            #3、修改操作
         */
        DeleteRequest delStudent = new DeleteRequest("student", "3");
        bulkRequest.add(delStudent);


        Map map = new HashMap();
        map.put("name", "zhangsan");
        map.put("age", "12");
        map.put("address", "银河系太阳系地球中华人民共和国");
        IndexRequest addStudent = new IndexRequest("student").id("6").source(map);
        bulkRequest.add(addStudent);

        UpdateRequest updateRequest = new UpdateRequest("student", "3").doc(map);
        bulkRequest.add(updateRequest);


        //执行批量操作
        BulkResponse bulk = client.bulk(bulkRequest, RequestOptions.DEFAULT);

        RestStatus status = bulk.status();

        System.out.println(status);
    }

    /**
     * 批量导入
     */
    @Test
    public void importData() throws IOException {
        //查询所有数据
        List<Goods> goodsList = goodsMapper.findAll();

        System.out.println(goodsList.size());

        //2、bulk导入
        BulkRequest bulkRequest = new BulkRequest();

        //2.1、循环goodlist 创建
        for (Goods goods : goodsList) {
            //2.2 设置其中的部分数据
            /*
            JSON.parseObject 将json字符串转成任意格式

             */
            goods.setSpec(JSON.parseObject(goods.getSpecStr(), Map.class));

            IndexRequest goods1 = new IndexRequest("goods").id(String.valueOf(goods.getId())).source(JSON.toJSONString(goods), XContentType.JSON);
            bulkRequest.add(goods1);
        }

        BulkResponse bulk = client.bulk(bulkRequest, RequestOptions.DEFAULT);

        System.out.println(bulk.status());
    }

    /**
     * 查询所有文档
     * matchall
     * 将查询结果封装到goods 装载到list中
     * 进行分页操作
     */
    @Test
    public void matchAll() throws IOException {
        //2、构建查询对象
        SearchRequest searchRequest = new SearchRequest("goods");

        //4、创建查询条件构建器
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        //5、查询条件 即用matchall的方式来查询es
        MatchAllQueryBuilder query = QueryBuilders.matchAllQuery();

        //6、指定查询条件
        searchSourceBuilder.query(query);

        //8、添加分页信息
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(300);

        //3、添加查询条件构建器
        searchRequest.source(searchSourceBuilder);

        //1、查询动作
        SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);

        //7、获取命中对象
        SearchHits hits = search.getHits();

        //7.1获取总记录数
        long totalHits = hits.getTotalHits().value;

        System.out.println("总记录数" + totalHits);

        //7.2获取hits中的数据
        SearchHit[] hits1 = hits.getHits();

        List<Goods> goodsList = new ArrayList();

        for (SearchHit documentFields : hits1) {
            //获取命中数组中的每个数组json字符串
            String sourceAsString = documentFields.getSourceAsString();

            //将json字符串转换成goods对象
            Goods goods = JSON.parseObject(sourceAsString, Goods.class);

            goodsList.add(goods);

        }

        System.out.println(goodsList);
    }

    /**
     * term 查询
     * 不分词查询条件查询
     */
    @Test
    public void testTerm() throws IOException {
        //2、构建查询对象
        SearchRequest searchRequest = new SearchRequest("goods");

        //4、创建查询条件构建器
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        //5、查询条件 即用matchall的方式来查询es
        TermQueryBuilder query = QueryBuilders.termQuery("title", "华为");

        //6、指定查询条件
        searchSourceBuilder.query(query);

        //8、添加分页信息
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(300);

        //3、添加查询条件构建器
        searchRequest.source(searchSourceBuilder);

        //1、查询动作
        SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);

        //7、获取命中对象
        SearchHits hits = search.getHits();

        //7.1获取总记录数
        long totalHits = hits.getTotalHits().value;

        System.out.println("总记录数" + totalHits);

        //7.2获取hits中的数据
        SearchHit[] hits1 = hits.getHits();

        List<Goods> goodsList = new ArrayList();

        for (SearchHit documentFields : hits1) {
            //获取命中数组中的每个数组json字符串
            String sourceAsString = documentFields.getSourceAsString();

            //将json字符串转换成goods对象
            Goods goods = JSON.parseObject(sourceAsString, Goods.class);

            goodsList.add(goods);

        }

        System.out.println(goodsList);
    }

    /**
     * match 查询
     * 对查询条件分词查询
     */
    @Test
    public void testMatch() throws IOException {
        //2、构建查询对象
        SearchRequest searchRequest = new SearchRequest("goods");

        //4、创建查询条件构建器
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        //5、查询条件 即用matchall的方式来查询es
        MatchQueryBuilder query = QueryBuilders.matchQuery("title", "华为手机");
        //指定分词之间的操作方式 and、or
        query.operator(Operator.AND);

        //6、指定查询条件
        searchSourceBuilder.query(query);

        //8、添加分页信息
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(300);

        //3、添加查询条件构建器
        searchRequest.source(searchSourceBuilder);

        //1、查询动作
        SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);

        //7、获取命中对象
        SearchHits hits = search.getHits();

        //7.1获取总记录数
        long totalHits = hits.getTotalHits().value;

        System.out.println("总记录数" + totalHits);

        //7.2获取hits中的数据
        SearchHit[] hits1 = hits.getHits();

        List<Goods> goodsList = new ArrayList();

        for (SearchHit documentFields : hits1) {
            //获取命中数组中的每个数组json字符串
            String sourceAsString = documentFields.getSourceAsString();

            //将json字符串转换成goods对象
            Goods goods = JSON.parseObject(sourceAsString, Goods.class);

            goodsList.add(goods);

        }

        System.out.println(goodsList);
    }

    /**
     * 模糊查询 查询
     * WildCard  分词之后的模糊查询
     */
    @Test
    public void testLikeWildCard() throws IOException {
        //2、构建查询对象
        SearchRequest searchRequest = new SearchRequest("goods");

        //4、创建查询条件构建器
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        //5、查询条件 即用matchall的方式来查询es
        WildcardQueryBuilder query = QueryBuilders.wildcardQuery("title", "华*");

        //6、指定查询条件
        searchSourceBuilder.query(query);

        //8、添加分页信息
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(300);

        //3、添加查询条件构建器
        searchRequest.source(searchSourceBuilder);

        //1、查询动作
        SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);

        //7、获取命中对象
        SearchHits hits = search.getHits();

        //7.1获取总记录数
        long totalHits = hits.getTotalHits().value;

        System.out.println("总记录数" + totalHits);

        //7.2获取hits中的数据
        SearchHit[] hits1 = hits.getHits();

        List<Goods> goodsList = new ArrayList();

        for (SearchHit documentFields : hits1) {
            //获取命中数组中的每个数组json字符串
            String sourceAsString = documentFields.getSourceAsString();

            //将json字符串转换成goods对象
            Goods goods = JSON.parseObject(sourceAsString, Goods.class);

            goodsList.add(goods);

        }

        System.out.println(goodsList);
    }

    /**
     * 正则查询 查询
     * 根据正则表达式查询
     */
    @Test
    public void testLikeRegexp() throws IOException {
        //2、构建查询对象
        SearchRequest searchRequest = new SearchRequest("goods");

        //4、创建查询条件构建器
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        //5、查询条件 即用matchall的方式来查询es
        RegexpQueryBuilder query = QueryBuilders.regexpQuery("title", "\\w+(.)*");

        //6、指定查询条件
        searchSourceBuilder.query(query);

        //8、添加分页信息
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(300);

        //3、添加查询条件构建器
        searchRequest.source(searchSourceBuilder);

        //1、查询动作
        SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);

        //7、获取命中对象
        SearchHits hits = search.getHits();

        //7.1获取总记录数
        long totalHits = hits.getTotalHits().value;

        System.out.println("总记录数" + totalHits);

        //7.2获取hits中的数据
        SearchHit[] hits1 = hits.getHits();

        List<Goods> goodsList = new ArrayList();

        for (SearchHit documentFields : hits1) {
            //获取命中数组中的每个数组json字符串
            String sourceAsString = documentFields.getSourceAsString();

            //将json字符串转换成goods对象
            Goods goods = JSON.parseObject(sourceAsString, Goods.class);

            goodsList.add(goods);

        }

        System.out.println(goodsList);
    }

    /**
     * 前缀查询 查询
     * 根据前缀查询 对于keyword的匹配度较高
     */
    @Test
    public void testLikePrefix() throws IOException {
        //2、构建查询对象
        SearchRequest searchRequest = new SearchRequest("goods");

        //4、创建查询条件构建器
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        //5、查询条件 即用matchall的方式来查询es
        PrefixQueryBuilder query = QueryBuilders.prefixQuery("brandName", "三");

        //6、指定查询条件
        searchSourceBuilder.query(query);

        //8、添加分页信息
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(300);

        //3、添加查询条件构建器
        searchRequest.source(searchSourceBuilder);

        //1、查询动作
        SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);

        //7、获取命中对象
        SearchHits hits = search.getHits();

        //7.1获取总记录数
        long totalHits = hits.getTotalHits().value;

        System.out.println("总记录数" + totalHits);

        //7.2获取hits中的数据
        SearchHit[] hits1 = hits.getHits();

        List<Goods> goodsList = new ArrayList();

        for (SearchHit documentFields : hits1) {
            //获取命中数组中的每个数组json字符串
            String sourceAsString = documentFields.getSourceAsString();

            //将json字符串转换成goods对象
            Goods goods = JSON.parseObject(sourceAsString, Goods.class);

            goodsList.add(goods);

        }

        System.out.println(goodsList);
    }

    /**
     * 范围 查询
     * 根据某字段的范围查询数据
     */
    @Test
    public void testRange() throws IOException {
        //2、构建查询对象
        SearchRequest searchRequest = new SearchRequest("goods");

        //4、创建查询条件构建器
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        //5、查询条件 即用范围查询的方式来查询es
        RangeQueryBuilder query = QueryBuilders.rangeQuery("price");

        //执行下限
        query.gte(2000);

        //执行上线
        query.lte(3000);


        //6、指定查询条件
        searchSourceBuilder.query(query);


        //对查询出的结果排序  降序排列
        searchSourceBuilder.sort("price", SortOrder.DESC);

        //8、添加分页信息
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(300);

        //3、添加查询条件构建器
        searchRequest.source(searchSourceBuilder);

        //1、查询动作
        SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);

        //7、获取命中对象
        SearchHits hits = search.getHits();

        //7.1获取总记录数
        long totalHits = hits.getTotalHits().value;

        System.out.println("总记录数" + totalHits);

        //7.2获取hits中的数据
        SearchHit[] hits1 = hits.getHits();

        List<Goods> goodsList = new ArrayList();

        for (SearchHit documentFields : hits1) {
            //获取命中数组中的每个数组json字符串
            String sourceAsString = documentFields.getSourceAsString();

            //将json字符串转换成goods对象
            Goods goods = JSON.parseObject(sourceAsString, Goods.class);

            goodsList.add(goods);

        }

        System.out.println(goodsList);
    }

    /**
     * 根据多字段查询 查询
     */
    @Test
    public void testQueryString() throws IOException {
        //2、构建查询对象
        SearchRequest searchRequest = new SearchRequest("goods");

        //4、创建查询条件构建器
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        //5、查询条件 使用对多字段进行or and的查询  field 代表产于查询的字段 可添加多个  defaultOperator 设置链接方式 OR AND
        QueryStringQueryBuilder query = QueryBuilders.queryStringQuery("华为手机").field("title").field("categoryName").field("brandName").defaultOperator(Operator.AND);


        //6、指定查询条件
        searchSourceBuilder.query(query);


        //对查询出的结果排序  降序排列
        searchSourceBuilder.sort("price", SortOrder.DESC);

        //8、添加分页信息
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(300);

        //3、添加查询条件构建器
        searchRequest.source(searchSourceBuilder);

        //1、查询动作
        SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);

        //7、获取命中对象
        SearchHits hits = search.getHits();

        //7.1获取总记录数
        long totalHits = hits.getTotalHits().value;

        System.out.println("总记录数" + totalHits);

        //7.2获取hits中的数据
        SearchHit[] hits1 = hits.getHits();

        List<Goods> goodsList = new ArrayList();

        for (SearchHit documentFields : hits1) {
            //获取命中数组中的每个数组json字符串
            String sourceAsString = documentFields.getSourceAsString();

            //将json字符串转换成goods对象
            Goods goods = JSON.parseObject(sourceAsString, Goods.class);

            goodsList.add(goods);

        }

        System.out.println(goodsList);
    }

    /**
     * 多条件查询 查询
     * 1、查询品牌为华为
     * 2、查询标题为手机
     * 3、查询价格在2000-3000之内的
     * 三个条件为并且
     */
    @Test
    public void testBoolQuery() throws IOException {
        //2、构建查询对象
        SearchRequest searchRequest = new SearchRequest("goods");

        //4、创建查询条件构建器
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        //5、查询条件 根据多条件查询
        BoolQueryBuilder query = QueryBuilders.boolQuery();

        //构建各个查询条件
        //品牌为华为条件构建  使用term的方式查询
        QueryBuilder termQuery = QueryBuilders.termQuery("brandName", "华为");
        query.must(termQuery);

        //查询标题为手机 使用match查询
        QueryBuilder matchQuery = QueryBuilders.matchQuery("title", "手机");
        query.filter(matchQuery);

        //查询价格在2000-3000之内的
        RangeQueryBuilder rangeQuery = QueryBuilders.rangeQuery("price");
        rangeQuery.gte(2000);
        rangeQuery.lte(3000);
        query.filter(rangeQuery);


        //6.使用boolQuery链接
        searchSourceBuilder.query(query);


        //对查询出的结果排序  降序排列
        searchSourceBuilder.sort("price", SortOrder.DESC);

        //8、添加分页信息
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(300);

        //3、添加查询条件构建器
        searchRequest.source(searchSourceBuilder);

        //1、查询动作
        SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);

        //7、获取命中对象
        SearchHits hits = search.getHits();

        //7.1获取总记录数
        long totalHits = hits.getTotalHits().value;

        System.out.println("总记录数" + totalHits);

        //7.2获取hits中的数据
        SearchHit[] hits1 = hits.getHits();

        List<Goods> goodsList = new ArrayList();

        for (SearchHit documentFields : hits1) {
            //获取命中数组中的每个数组json字符串
            String sourceAsString = documentFields.getSourceAsString();

            //将json字符串转换成goods对象
            Goods goods = JSON.parseObject(sourceAsString, Goods.class);

            goodsList.add(goods);

        }

        System.out.println(goodsList);
    }

    /**
     * 聚合查询
     * 1、查询标包含手机的数据
     * 2、分组查询手机品牌
     */
    @Test
    public void testAggQuery() throws IOException {
        //2、构建查询对象
        SearchRequest searchRequest = new SearchRequest("goods");

        //4、创建查询条件构建器
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();


        //查询标题为手机 使用match查询
        QueryBuilder query = QueryBuilders.matchQuery("title", "手机");

        //分组查询手机品牌 terms 自定义的名称 用来获取数据使用 field 用来已哪个字段分组
        AggregationBuilder agg = AggregationBuilders.terms("goods_brands").field("brandName").size(100);
        searchSourceBuilder.aggregation(agg);


        //6.使用boolQuery链接
        searchSourceBuilder.query(query);


        //对查询出的结果排序  降序排列
        searchSourceBuilder.sort("price", SortOrder.DESC);

        //8、添加分页信息
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(300);

        //3、添加查询条件构建器
        searchRequest.source(searchSourceBuilder);

        //1、查询动作
        SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);

        //7、获取命中对象
        SearchHits hits = search.getHits();

        //7.1获取总记录数
        long totalHits = hits.getTotalHits().value;

        System.out.println("总记录数" + totalHits);

        //7.2获取hits中的数据
        SearchHit[] hits1 = hits.getHits();

        List<Goods> goodsList = new ArrayList();

        for (SearchHit documentFields : hits1) {
            //获取命中数组中的每个数组json字符串
            String sourceAsString = documentFields.getSourceAsString();

            //将json字符串转换成goods对象
            Goods goods = JSON.parseObject(sourceAsString, Goods.class);

            goodsList.add(goods);

        }

        //获取聚合记过
        Aggregations aggregations = search.getAggregations();

        Map<String, Aggregation> asMap = aggregations.getAsMap();

        Terms goods_brands = (Terms) asMap.get("goods_brands");

        List<? extends Terms.Bucket> buckets = goods_brands.getBuckets();

        List brands = new ArrayList();
        for (Terms.Bucket bucket : buckets) {
            Object key = bucket.getKey();

            brands.add(key);
        }

        System.out.println(brands);

//        System.out.println(goodsList);
    }

    /**
     * 高亮查询
     * 1、设置高亮
     * 高亮字段
     * 前缀
     * 后缀
     * 2、将高亮字段替换原有字段
     */
    @Test
    public void testHightQuery() throws IOException {
        //2、构建查询对象
        SearchRequest searchRequest = new SearchRequest("goods");

        //4、创建查询条件构建器
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();


        //查询标题为手机 使用match查询
        QueryBuilder query = QueryBuilders.matchQuery("title", "手机");


        //6.使用boolQuery链接
        searchSourceBuilder.query(query);

        //设置高亮
        HighlightBuilder hight = new HighlightBuilder();
        //设置三要素
        hight.field("title");
        hight.preTags("<font color='red'>");
        hight.postTags("</font>");


        searchSourceBuilder.highlighter(hight);


        //3、添加查询条件构建器
        searchRequest.source(searchSourceBuilder);

        //1、查询动作
        SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);

        //7、获取命中对象
        SearchHits hits = search.getHits();

        //7.1获取总记录数
        long totalHits = hits.getTotalHits().value;

        System.out.println("总记录数" + totalHits);

        //7.2获取hits中的数据
        SearchHit[] hits1 = hits.getHits();

        List<Goods> goodsList = new ArrayList();

        for (SearchHit documentFields : hits1) {
            //获取命中数组中的每个数组json字符串
            String sourceAsString = documentFields.getSourceAsString();

            //将json字符串转换成goods对象
            Goods goods = JSON.parseObject(sourceAsString, Goods.class);

            //获取高亮结果 替换goods中的title
            Map<String, HighlightField> highlightFields = documentFields.getHighlightFields();

            HighlightField highlightField = highlightFields.get("title");

            //获取片段
            Text[] fragments = highlightField.fragments();

            //替换
            goods.setTitle(fragments[0].toString());

            goodsList.add(goods);

        }

        System.out.println(goodsList);
    }
}
