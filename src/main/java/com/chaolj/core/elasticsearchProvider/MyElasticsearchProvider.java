package com.chaolj.core.elasticsearchProvider;

import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSON;
import com.chaolj.core.MyApp;
import com.chaolj.core.commonUtils.myDto.QueryDataDto;
import com.chaolj.core.commonUtils.myDto.UIException;
import com.chaolj.core.commonUtils.myServer.Interface.IElasticsearchServer;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.PutMappingRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.*;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.context.ApplicationContext;
import java.sql.Timestamp;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class MyElasticsearchProvider implements IElasticsearchServer {
    private ApplicationContext ctx;
    private MyElasticsearchProviderProperties properties;

    private RestHighLevelClient client() {
        return this.ctx.getBean(RestHighLevelClient.class);
    }

    public MyElasticsearchProvider(ApplicationContext applicationContext, MyElasticsearchProviderProperties providerProperties) {
        this.ctx = applicationContext;
        this.properties = providerProperties;
    }

    private void resetDataMapInit(Map<String,Object> dataMap, String idFieldName) {
        if (dataMap == null) return;

        // region 初始化Id

        idFieldName = StrUtil.isBlank(idFieldName) ? this.properties.getIdFieldName() : idFieldName;

        if (!dataMap.containsKey(idFieldName)) {
            dataMap.put(idFieldName, MyApp.Helper().GuidHelper().NewID());
        }
        else if (dataMap.get(idFieldName) == null) {
            dataMap.put(idFieldName, MyApp.Helper().GuidHelper().NewID());
        }
        else if (StrUtil.isBlank(dataMap.get(idFieldName).toString())) {
            dataMap.put(idFieldName, MyApp.Helper().GuidHelper().NewID());
        }

        // endregion

        // region 初始化LastTime

        var now = LocalDateTime.now();

        if (!dataMap.containsKey(this.properties.getTimeFieldName())) {
            dataMap.put(this.properties.getTimeFieldName(), now);
        }
        else if (dataMap.get(this.properties.getTimeFieldName()) == null) {
            dataMap.put(this.properties.getTimeFieldName(), now);
        }
        else if (StrUtil.isBlank(dataMap.get(this.properties.getTimeFieldName()).toString())) {
            dataMap.put(this.properties.getTimeFieldName(), now);
        }

        // endregion
    }

    private  void resetDataMapDateTimeWrite(Map<String,Object> dataMap) {
        if (dataMap == null) return;

        dataMap.forEach((key, value) -> {
            if (value == null) return;

            if (value instanceof LocalDateTime) {
                var isoTime = ((LocalDateTime)value).atZone(ZoneId.systemDefault()).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
                dataMap.put(key, isoTime);
                return;
            }

            if (value instanceof LocalDate) {
                var isoTime = ((LocalDate)value).atTime(0,0,0).atZone(ZoneId.systemDefault()).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
                dataMap.put(key, isoTime);
                return;
            }

            if (value instanceof Timestamp) {
                var isoTime = ((Timestamp)value).toLocalDateTime().atZone(ZoneId.systemDefault()).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
                dataMap.put(key, isoTime);
                return;
            }

            if (value instanceof Date) {
                var isoTime = ((Date)value).toInstant().atZone(ZoneId.systemDefault()).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
                dataMap.put(key, isoTime);
                return;
            }

            if (value instanceof ZonedDateTime) {
                var isoTime = ((ZonedDateTime)value).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
                dataMap.put(key, isoTime);
                return;
            }

            if (value instanceof Instant) {
                var isoTime = ((Instant)value).atZone(ZoneId.systemDefault()).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
                dataMap.put(key, isoTime);
                return;
            }

            if (value instanceof String) {
                var sValue = (String) value;
                if (StrUtil.isNotBlank(sValue) && sValue.length() >= 10 && sValue.charAt(4) == '-' && sValue.charAt(7) == '-') {
                    var isoDateTime = MyApp.Of(sValue).ToLocalDateTime(null);
                    if (isoDateTime != null) {
                        dataMap.put(key, isoDateTime.atZone(ZoneId.systemDefault()).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
                        return;
                    }
                }
            }
        });
    }

    private void resetDataMapDateTimeRead(Map<String,Object> dataMap) {
        if (dataMap == null) return;

        // region 时间

        dataMap.forEach((key, value) -> {
            if (value == null) return;

            if (value instanceof String) {
                var sValue = (String) value;
                if (StrUtil.isNotBlank(sValue) && sValue.length() >= 10 && sValue.charAt(4) == '-' && sValue.charAt(7) == '-') {
                    var isoDateTime = MyApp.Of(sValue).ToLocalDateTime(null);
                    if (isoDateTime != null) {
                        dataMap.put(key, isoDateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
                        return;
                    }
                }
            }
        });

        // endregion
    }

    @Override
    public boolean checkIndex(String indexName) {
        if (StrUtil.isBlank(indexName)) return false;

        try {
            return this.client().indices().exists(new GetIndexRequest(indexName), RequestOptions.DEFAULT);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public void createIndex(String indexName, Map<String, Object> indexMap) {
        if (StrUtil.isBlank(indexName)) return;
        if(checkIndex(indexName)) return;

        try {
            var request = new CreateIndexRequest(indexName);

            // 设置分片和副本
            request.settings(Settings.builder()
                    .put("index.number_of_shards", 5)
                    .put("index.number_of_replicas", 1)
            );

            // 设置索引映射
            if (indexMap != null && indexMap.size()>0) {
                Map<String, Object> source = new HashMap<>();
                source.put("properties", indexMap);
                request.mapping(source);
            }

            var response = this.client().indices().create(request, RequestOptions.DEFAULT);

            if (!response.isAcknowledged()) {
                throw new RuntimeException("createIndex.response.isAcknowledged:false，indexName[" + indexName + "]");
            }
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public void putIndexMapping(String indexName, Map<String, Object> indexMap) {
        if (StrUtil.isBlank(indexName)) return;
        if(!checkIndex(indexName)) return;

        try {
            var request = new PutMappingRequest(indexName);

            // 设置索引映射
            if (indexMap != null && indexMap.size()>0) {
                Map<String, Object> source = new HashMap<>();
                source.put("properties", indexMap);
                request.source(source);
            }

            var response = this.client().indices().putMapping(request, RequestOptions.DEFAULT);

            if (!response.isAcknowledged()) {
                throw new RuntimeException("updateIndexMapping.response.isAcknowledged:false，indexName[" + indexName + "]");
            }
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public void deleteIndex(String indexName) {
        if (StrUtil.isBlank(indexName)) return;
        if(!checkIndex(indexName)) return;

        try {
            var request = new DeleteIndexRequest(indexName);
            var response = this.client().indices().delete(request, RequestOptions.DEFAULT);
            if (!response.isAcknowledged()) {
                throw new RuntimeException("deleteIndex.response.isAcknowledged:false，indexName[" + indexName + "]");
            }
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public void saveDoc(String indexName, Map<String, Object> dataMap, String idFieldName) {
        if (StrUtil.isBlank(indexName)) return;

        idFieldName = !StrUtil.isBlank(idFieldName) ? idFieldName : this.properties.getIdFieldName();

        try {
            this.resetDataMapInit(dataMap, idFieldName);
            this.resetDataMapDateTimeWrite(dataMap);

            var request = new BulkRequest();
            request.timeout(TimeValue.timeValueMillis(this.properties.getRequestTimeout()));
            request.add(new IndexRequest(indexName,"_doc")
                    .id(dataMap.get(idFieldName).toString())
                    .opType(DocWriteRequest.OpType.INDEX)
                    .source(dataMap, XContentType.JSON));
            var response = this.client().bulk(request, RequestOptions.DEFAULT);

            if (response.hasFailures()) {
                throw new RuntimeException("saveDoc.response.status:" + response.status().toString()
                        + ",indexName[" + indexName + "]"
                        + ",Id[" + dataMap.get(idFieldName).toString() + "]"
                        + ",FailureMessage[" + response.buildFailureMessage() + "]");
            }
        } catch (Exception e){
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public void saveDoc(String indexName, Map<String, Object> dataMap){
        this.saveDoc(indexName, dataMap, this.properties.getIdFieldName());
    }

    @Override
    public void batchSaveDoc(String indexName, List<Map<String, Object>> dataMaps, String idFieldName) {
        if (StrUtil.isBlank(indexName)) return;
        if (dataMaps == null) return;
        if (dataMaps.isEmpty()) return;

        idFieldName = !StrUtil.isBlank(idFieldName) ? idFieldName : this.properties.getIdFieldName();

        try {
            var request = new BulkRequest();
            request.timeout(TimeValue.timeValueMillis(this.properties.getRequestTimeout()));
            for (var dataMap : dataMaps){
                this.resetDataMapInit(dataMap, idFieldName);
                this.resetDataMapDateTimeWrite(dataMap);

                request.add(new IndexRequest(indexName,"_doc")
                        .id(dataMap.get(idFieldName).toString())
                        .opType(DocWriteRequest.OpType.INDEX)
                        .source(dataMap,XContentType.JSON));
            }
            var response = this.client().bulk(request, RequestOptions.DEFAULT);

            if (response.hasFailures()) {
                throw new RuntimeException("batchSaveDoc.response.status:" + response.status().toString()
                        + ",indexName[" + indexName + "]"
                        + ",FailureMessage[" + response.buildFailureMessage() + "]");
            }
        } catch (Exception e){
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public void batchSaveDoc(String indexName, List<Map<String, Object>> dataMaps){
        this.batchSaveDoc(indexName, dataMaps, this.properties.getIdFieldName());
    }

    @Override
    public void updateDoc(String indexName, String id, Map<String, Object> dataMap) {
        if (StrUtil.isBlank(indexName)) return;
        if (StrUtil.isBlank(id)) return;

        try {
            var request = new BulkRequest();
            request.timeout(TimeValue.timeValueMillis(this.properties.getRequestTimeout()));
            request.add(new UpdateRequest(indexName, id).doc(dataMap));

            var response = this.client().bulk(request, RequestOptions.DEFAULT);
            if (response.hasFailures()) {
                throw new RuntimeException("updateDoc.response.status:" + response.status().toString()
                        + ",indexName[" + indexName + "]"
                        + ",Id[" + id + "]"
                        + ",FailureMessage[" + response.buildFailureMessage() + "]");
            }
        } catch (Exception e){
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public void deleteDoc(String indexName, String id) {
        if (StrUtil.isBlank(indexName)) return;
        if (StrUtil.isBlank(id)) return;

        try {
            var request = new BulkRequest();
            request.timeout(TimeValue.timeValueMillis(this.properties.getRequestTimeout()));
            request.add(new DeleteRequest(indexName,"_doc").id(id));

            var response = this.client().bulk(request, RequestOptions.DEFAULT);
            if (response.hasFailures()) {
                throw new RuntimeException("deleteDoc.response.status:" + response.status().toString()
                        + ",indexName[" + indexName + "]"
                        + ",Id[" + id + "]"
                        + ",FailureMessage[" + response.buildFailureMessage() + "]");
            }
        } catch (Exception e){
            throw new RuntimeException(e.getMessage());
        }
    }

    public void batchDeleteDoc(String indexName, QueryBuilder queryBuilder) {
        if (StrUtil.isBlank(indexName)) return;

        var request = new DeleteByQueryRequest(indexName);
        request.setTimeout(TimeValue.timeValueMillis(this.properties.getRequestTimeout()));
        request.setQuery(queryBuilder);

        try {
            var response = this.client().deleteByQuery(request, RequestOptions.DEFAULT);
            var failures = response.getBulkFailures();
            if (failures != null && !failures.isEmpty()) {
                throw new RuntimeException("batchDeleteDoc.response.status:" + response.getStatus().toString()
                        + ",indexName[" + indexName + "]"
                        + ",failures[" + JSON.toJSONString(failures.stream().map(m -> m.getMessage()).collect(Collectors.toList())) + "]");
            }
        } catch (Exception e){
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public void batchDeleteDoc(String indexName, List<String> ids) {
        if (StrUtil.isBlank(indexName)) return;
        if (ids == null) return;
        if (ids.isEmpty()) return;

        var boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(QueryBuilders.idsQuery().addIds(ids.toArray(new String[0])));
        this.batchDeleteDoc(indexName, boolQueryBuilder);
    }

    @Override
    public void batchDeleteDoc(String indexName, QueryDataDto query) {
        if (StrUtil.isBlank(indexName)) return;

        var queryBuilder = this.convertToQueryBuilder(query);
        this.batchDeleteDoc(indexName, queryBuilder);
    }

    @Override
    public Map<String, Object> getDoc(String indexName, String id) {
        if (StrUtil.isBlank(indexName)) return null;
        if (StrUtil.isBlank(id)) return null;

        var request = new GetRequest(indexName, id);
        try {
            var response = this.client().get(request, RequestOptions.DEFAULT);
            if (response.isExists()) {
                var dataMap = response.getSourceAsMap();
                this.resetDataMapDateTimeRead(dataMap);
                return dataMap;
            }
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }

        return null;
    }

    @Override
    public List<Map<String, Object>> getDoc(String indexName, List<String> ids) {
        if (StrUtil.isBlank(indexName)) return new ArrayList<>();
        if (ids == null) return new ArrayList<>();
        if (ids.isEmpty()) return new ArrayList<>();

        var boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(QueryBuilders.idsQuery().addIds(ids.toArray(new String[0])));
        return this.queryList(new String[]{indexName}, boolQueryBuilder, "*");
    }

    public Long queryCount(String[] indices, QueryBuilder queryBuilder) {
        if (indices == null) return 0l;
        if (indices.length <= 0) return 0l;

        var request = new CountRequest(indices);
        request.query(queryBuilder);

        try {
            var response = this.client().count(request, RequestOptions.DEFAULT);
            var failures = response.getShardFailures();
            if (failures != null && failures.length > 0) {
                throw new RuntimeException("queryCount.response.status:" + response.status().toString()
                        + ",indexName" + Arrays.toString(indices)
                        + ",failures[" + JSON.toJSONString(failures) + "]");
            }
            return response.getCount();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public Long queryCount(String[] indices, QueryDataDto query) {
        if (indices == null) return 0l;
        if (indices.length <= 0) return 0l;

        var queryBuilder = this.convertToQueryBuilder(query);
        return this.queryCount(indices, queryBuilder);
    }

    public List<Map<String, Object>> queryList(String[] indices, SearchSourceBuilder sourceBuilder) {
        var data = new ArrayList<Map<String,Object>>();

        if (indices == null) return data;
        if (indices.length <= 0) return data;

        var searchRequest = new SearchRequest(indices);
        searchRequest.scroll(TimeValue.timeValueMinutes(3L));
        searchRequest.source(sourceBuilder);

        try {
            var response = this.client().search(searchRequest, RequestOptions.DEFAULT);

            var failures = response.getShardFailures();
            if (failures != null && failures.length > 0) {
                throw new RuntimeException("queryList.response.status:" + response.status().toString()
                        + ",indexName" + Arrays.toString(searchRequest.indices())
                        + ",failures[" + JSON.toJSONString(failures) + "]");
            }

            var scrollId = response.getScrollId();
            var searchHitArr = response.getHits().getHits();
            for (var searchHit : searchHitArr){
                var dataMap = searchHit.getSourceAsMap();
                this.resetDataMapDateTimeRead(dataMap);
                data.add(dataMap);
            }

            var searchEnd = searchHitArr == null || searchHitArr.length <= 0;
            while (!searchEnd) {
                var scrollRequest = new SearchScrollRequest(scrollId);
                scrollRequest.scroll(TimeValue.timeValueMinutes(3L));
                var scrollResponse = this.client().scroll(scrollRequest, RequestOptions.DEFAULT);
                var scrollFailures = scrollResponse.getShardFailures();
                if (scrollFailures != null && scrollFailures.length > 0) {
                    throw new RuntimeException("queryList.response.status:" + scrollResponse.status().toString()
                            + ",indexName" + Arrays.toString(searchRequest.indices())
                            + ",failures[" + JSON.toJSONString(scrollFailures) + "]");
                }

                var scrollHitArr = scrollResponse.getHits().getHits();
                for (var scrollHit : scrollHitArr){
                    var dataMap = scrollHit.getSourceAsMap();
                    this.resetDataMapDateTimeRead(dataMap);
                    data.add(dataMap);
                }

                searchEnd = scrollHitArr == null || scrollHitArr.length <= 0;
            }

            var clearRequest = new ClearScrollRequest();
            clearRequest.addScrollId(scrollId);
            this.client().clearScroll(clearRequest, RequestOptions.DEFAULT);

            return data;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    public List<Map<String, Object>> queryList(String[] indices, QueryBuilder queryBuilder, String source) {
        if (indices == null) return new ArrayList<>();
        if (indices.length <= 0) return new ArrayList<>();

        var sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.size(2000);
        sourceBuilder.query(queryBuilder);

        if (StrUtil.isNotBlank(source)) {
            sourceBuilder.fetchSource(MyApp.Of(source).ToArray(","), null);
        }

        return this.queryList(indices, sourceBuilder);
    }

    public List<Map<String, Object>> queryList(String[] indices, QueryBuilder queryBuilder) {
        return this.queryList(indices, queryBuilder, "");
    }

    @Override
    public List<Map<String, Object>> queryList(String[] indices, QueryDataDto query, String source) {
        if (indices == null) return new ArrayList<>();
        if (indices.length <= 0) return new ArrayList<>();

        var queryBuilder = this.convertToQueryBuilder(query);
        return this.queryList(indices, queryBuilder, source);
    }

    public List<Map<String, Object>> queryList(String[] indices, QueryDataDto query) {
        return this.queryList(indices, query, "");
    }

    public List<Map<String, Object>> queryPage(SearchRequest searchRequest) {
        var data = new ArrayList<Map<String,Object>>();

        try {
            var response = this.client().search(searchRequest, RequestOptions.DEFAULT);
            var failures = response.getShardFailures();
            if (failures != null && failures.length > 0) {
                throw new RuntimeException("queryPage.response.status:" + response.status().toString()
                        + ",indexName" + Arrays.toString(searchRequest.indices())
                        + ",failures[" + JSON.toJSONString(failures) + "]");
            }

            var searchHitArr = response.getHits().getHits();
            for (var searchHit : searchHitArr){
                var dataMap = searchHit.getSourceAsMap();
                this.resetDataMapDateTimeRead(dataMap);
                data.add(dataMap);
            }
            return data;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    public List<Map<String, Object>> queryPage(String[] indices, SearchSourceBuilder sourceBuilder) {
        if (indices == null) return new ArrayList<>();
        if (indices.length <= 0) return new ArrayList<>();

        var searchRequest = new SearchRequest(indices);
        searchRequest.source(sourceBuilder);
        return this.queryPage(searchRequest);
    }

    public List<Map<String, Object>> queryPage(String[] indices, QueryBuilder queryBuilder, int page, int rows, SortBuilder<?>... sorts) {
        if (indices == null) return new ArrayList<>();
        if (indices.length <= 0) return new ArrayList<>();

        var pageIndex = page <= 0 ? 1 : page;
        var pageRows = rows <= 0 ? 20 : rows;

        var sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(queryBuilder);
        sourceBuilder.from((pageIndex - 1) * pageRows);
        sourceBuilder.size(pageRows);

        if (sorts != null && sorts.length > 0) {
            for (var s : sorts) sourceBuilder.sort(s);
        }

        return this.queryPage(indices, sourceBuilder);
    }

    @Override
    public List<Map<String, Object>> queryPage(String[] indices, QueryDataDto query, String source) {
        if (indices == null) return new ArrayList<>();
        if (indices.length <= 0) return new ArrayList<>();

        var queryBuilder = this.convertToQueryBuilder(query);
        var sourceBuilder = this.convertToSearchSourceBuilder(query);
        sourceBuilder.query(queryBuilder);

        if (StrUtil.isNotBlank(source)) {
            sourceBuilder.fetchSource(MyApp.Of(source).ToArray(","), null);
        }

        return this.queryPage(indices, sourceBuilder);
    }

    public List<Map<String, Object>> queryPage(String[] indices, QueryDataDto query) {
        return this.queryPage(indices, query, "");
    }

    private QueryBuilder convertToQueryBuilder(QueryDataDto query) {
        var queryBuilder = QueryBuilders.boolQuery();

        var ands = query.getWhereAnd();
        if (ands != null && !ands.isEmpty()) {
            ands.forEach(where -> {
                var fieldName = where.getField();
                var operation = where.getOperation();

                if (StrUtil.isBlank(fieldName)) throw new UIException("查询字段名不能为空: " + where);
                if (StrUtil.isBlank(operation)) throw new UIException("查询操作符不能为空: " + where);

                fieldName = fieldName.trim();
                operation = operation.trim();

                var fieldValue = where.getValue();
                if (StrUtil.isNotBlank(fieldValue) && fieldValue.length() >= 10 && fieldValue.charAt(4) == '-' && fieldValue.charAt(7) == '-') {
                    var isoDateTime = MyApp.Of(fieldValue).ToLocalDateTime(null);
                    if (isoDateTime != null) {
                        fieldValue = MyApp.Of(isoDateTime).ToFormat(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
                    }
                }

                var fieldNameKeyword = MyApp.Of(fieldName).AppendSuffix(".keyword").ToStr();
                if (operation.equals("=") || operation.equals("==")) {
                    queryBuilder.must(QueryBuilders.termQuery(fieldNameKeyword, fieldValue));
                }
                else if (operation.equals("<>") || operation.equals("!=")) {
                    queryBuilder.mustNot(QueryBuilders.termQuery(fieldNameKeyword, fieldValue));
                }
                else if (operation.equalsIgnoreCase("Like")) {
                    queryBuilder.must(QueryBuilders.wildcardQuery(fieldName, "*" + fieldValue + "*"));
                }
                else if (operation.equalsIgnoreCase("StartsWith") || operation.equalsIgnoreCase("LikeRight")) {
                    queryBuilder.must(QueryBuilders.wildcardQuery(fieldName, fieldValue + "*"));
                }
                else if (operation.equalsIgnoreCase("EndsWith") || operation.equalsIgnoreCase("LikeLeft")) {
                    queryBuilder.must(QueryBuilders.wildcardQuery(fieldName, "*" + fieldValue));
                }
                else if (operation.equalsIgnoreCase("In")) {
                    var vals = StrUtil.splitToArray(fieldValue, ",");
                    queryBuilder.must(QueryBuilders.termsQuery(fieldName, vals));
                }
                else if (operation.equals(">")) {
                    queryBuilder.must(QueryBuilders.rangeQuery(fieldName).gt(fieldValue));
                }
                else if (operation.equals(">=")) {
                    queryBuilder.must(QueryBuilders.rangeQuery(fieldName).gte(fieldValue));
                }
                else if (operation.equals("<")) {
                    queryBuilder.must(QueryBuilders.rangeQuery(fieldName).lt(fieldValue));
                }
                else if (operation.equals("<=")) {
                    queryBuilder.must(QueryBuilders.rangeQuery(fieldName).lte(fieldValue));
                }
                else if (operation.equalsIgnoreCase("match")) {
                    queryBuilder.must(QueryBuilders.matchQuery(fieldName, fieldValue).operator(Operator.AND));
                }
                else if (operation.equalsIgnoreCase("match_phrase")) {
                    queryBuilder.must(QueryBuilders.matchPhraseQuery(fieldName, fieldValue));
                }
                else if (operation.equalsIgnoreCase("multi_match")) {
                    var vals = StrUtil.splitToArray(fieldName, ",");
                    queryBuilder.must(QueryBuilders.multiMatchQuery(fieldValue, vals)
                            .type(MultiMatchQueryBuilder.Type.CROSS_FIELDS)
                            .operator(Operator.AND));
                }
                else {
                    throw new UIException("查询操作符不合法: " + where);
                }
            });
        }

        var ors = query.getWhereOr();
        if (ors != null && !ors.isEmpty()) {
            ors.forEach(where -> {
                var fieldName = where.getField();
                var operation = where.getOperation();

                if (StrUtil.isBlank(fieldName)) throw new UIException("查询字段名不能为空: " + where);
                if (StrUtil.isBlank(operation)) throw new UIException("查询操作符不能为空: " + where);

                fieldName = fieldName.trim();
                operation = operation.trim();

                var fieldValue = where.getValue();
                if (StrUtil.isNotBlank(fieldValue) && fieldValue.length() >= 10 && fieldValue.charAt(4) == '-' && fieldValue.charAt(7) == '-') {
                    var isoDateTime = MyApp.Of(fieldValue).ToLocalDateTime(null);
                    if (isoDateTime != null) {
                        fieldValue = MyApp.Of(isoDateTime).ToFormat(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
                    }
                }

                var fieldNameKeyword = MyApp.Of(fieldName).AppendSuffix(".keyword").ToStr();
                if (operation.equals("=") || operation.equals("==")) {
                    queryBuilder.should(QueryBuilders.termQuery(fieldNameKeyword, fieldValue));
                }
                else if (operation.equals("<>") || operation.equals("!=")) {
                    queryBuilder.should(QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery(fieldNameKeyword, fieldValue)));
                }
                else if (operation.equalsIgnoreCase("Like")) {
                    queryBuilder.should(QueryBuilders.wildcardQuery(fieldName, "*" + fieldValue + "*"));
                }
                else if (operation.equalsIgnoreCase("StartsWith") || operation.equalsIgnoreCase("LikeRight")) {
                    queryBuilder.should(QueryBuilders.wildcardQuery(fieldName, fieldValue + "*"));
                }
                else if (operation.equalsIgnoreCase("EndsWith") || operation.equalsIgnoreCase("LikeLeft")) {
                    queryBuilder.should(QueryBuilders.wildcardQuery(fieldName, "*" + fieldValue));
                }
                else if (operation.equalsIgnoreCase("In")) {
                    var vals = StrUtil.splitToArray(fieldValue, ",");
                    queryBuilder.should(QueryBuilders.termQuery(fieldName, vals));
                }
                else if (operation.equals(">")) {
                    queryBuilder.should(QueryBuilders.rangeQuery(fieldName).gt(fieldValue));
                }
                else if (operation.equals(">=")) {
                    queryBuilder.should(QueryBuilders.rangeQuery(fieldName).gte(fieldValue));
                }
                else if (operation.equals("<")) {
                    queryBuilder.should(QueryBuilders.rangeQuery(fieldName).lt(fieldValue));
                }
                else if (operation.equals("<=")) {
                    queryBuilder.should(QueryBuilders.rangeQuery(fieldName).lte(fieldValue));
                }
                else if (operation.equalsIgnoreCase("match")) {
                    queryBuilder.should(QueryBuilders.matchQuery(fieldName, fieldValue).operator(Operator.AND));
                }
                else if (operation.equalsIgnoreCase("match_phrase")) {
                    queryBuilder.should(QueryBuilders.matchPhraseQuery(fieldName, fieldValue));
                }
                else if (operation.equalsIgnoreCase("multi_match")) {
                    var vals = StrUtil.splitToArray(fieldName, ",");
                    queryBuilder.should(QueryBuilders.multiMatchQuery(fieldValue, vals)
                            .type(MultiMatchQueryBuilder.Type.CROSS_FIELDS)
                            .operator(Operator.AND));
                }
                else {
                    throw new UIException("查询操作符不合法: " + where);
                }
            });
        }

        return queryBuilder;
    }

    private SearchSourceBuilder convertToSearchSourceBuilder(QueryDataDto query) {
        var pageIndex = query.getPage() <= 0 ? 1 : query.getPage();
        var pageRows = query.getRows() <= 0 ? 20 : query.getRows();

        var sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.from((pageIndex - 1) * pageRows);
        sourceBuilder.size(pageRows);

        var sorts = StrUtil.splitToArray(query.getSort(), ',');
        for (var sort : sorts) {
            if(StrUtil.isBlank(sort)) continue;

            var _sort = StrUtil.trim(sort);
            if(StrUtil.endWithIgnoreCase(_sort, " asc")){
                _sort = StrUtil.removeSuffixIgnoreCase(_sort, " asc").trim();
                if(StrUtil.isBlank(_sort)) continue;

                sourceBuilder.sort(SortBuilders.fieldSort(_sort).order(SortOrder.ASC));
            }
            else if(StrUtil.endWithIgnoreCase(_sort, " desc")){
                _sort = StrUtil.removeSuffixIgnoreCase(_sort, " desc").trim();
                if(StrUtil.isBlank(_sort)) continue;

                sourceBuilder.sort(SortBuilders.fieldSort(_sort).order(SortOrder.DESC));
            }
            else {
                sourceBuilder.sort(SortBuilders.fieldSort(_sort).order(SortOrder.ASC));
            }
        }

        return sourceBuilder;
    }
}
