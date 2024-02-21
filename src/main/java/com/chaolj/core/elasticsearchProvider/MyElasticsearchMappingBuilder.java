package com.chaolj.core.elasticsearchProvider;

import java.util.HashMap;
import java.util.Map;

public class MyElasticsearchMappingBuilder {
    private final String idFieldName = "Id";
    private final String timeFieldName = "LastTime";

    private Map<String, Object> properties;

    public MyElasticsearchMappingBuilder() {
        this.properties = new HashMap<>();
    }

    private Map<String, Object> MappingTypeEnum(MyElasticsearchMappingEnums proptype) {
        var value = new HashMap<String, Object>();
        value.put("type", proptype.getName());

        if (proptype == MyElasticsearchMappingEnums._date) {
            value.put("format", "yyy-MM-dd HH:mm:ss||yyyy-MM-dd||strict_date_optional_time||epoch_millis");
        }

        if (proptype == MyElasticsearchMappingEnums._text) {
            var keyword_0 = new HashMap<String, Object>();
            keyword_0.put("type", "keyword");
            keyword_0.put("ignore_above", "256");

            var keyword_1 = new HashMap<String, Object>();
            keyword_1.put("keyword", keyword_0);

            value.put("fields", keyword_1);
        }

        return value;
    }

    private Map<String, Object> MappingTypeNested(Map<String, Object> nestedProperties) {
        var value = new HashMap<String, Object>();
        value.put("type", "nested");
        value.put("properties", nestedProperties);
        return value;
    }

    public MyElasticsearchMappingBuilder propText(String fieldName) {
        this.properties.put(fieldName, this.MappingTypeEnum(MyElasticsearchMappingEnums._text));
        return this;
    }

    public MyElasticsearchMappingBuilder propKeyword(String fieldName) {
        this.properties.put(fieldName, this.MappingTypeEnum(MyElasticsearchMappingEnums._keyword));
        return this;
    }

    public MyElasticsearchMappingBuilder propDate(String fieldName) {
        this.properties.put(fieldName, this.MappingTypeEnum(MyElasticsearchMappingEnums._date));
        return this;
    }

    public MyElasticsearchMappingBuilder propBoolean(String fieldName) {
        this.properties.put(fieldName, this.MappingTypeEnum(MyElasticsearchMappingEnums._boolean));
        return this;
    }

    public MyElasticsearchMappingBuilder propDouble(String fieldName) {
        this.properties.put(fieldName, this.MappingTypeEnum(MyElasticsearchMappingEnums._double));
        return this;
    }

    public MyElasticsearchMappingBuilder propFloat(String fieldName) {
        this.properties.put(fieldName, this.MappingTypeEnum(MyElasticsearchMappingEnums._float));
        return this;
    }

    public MyElasticsearchMappingBuilder propByte(String fieldName) {
        this.properties.put(fieldName, this.MappingTypeEnum(MyElasticsearchMappingEnums._byte));
        return this;
    }

    public MyElasticsearchMappingBuilder propShort(String fieldName) {
        this.properties.put(fieldName, this.MappingTypeEnum(MyElasticsearchMappingEnums._short));
        return this;
    }

    public MyElasticsearchMappingBuilder propInteger(String fieldName) {
        this.properties.put(fieldName, this.MappingTypeEnum(MyElasticsearchMappingEnums._integer));
        return this;
    }

    public MyElasticsearchMappingBuilder propLong(String fieldName) {
        this.properties.put(fieldName, this.MappingTypeEnum(MyElasticsearchMappingEnums._long));
        return this;
    }

    public MyElasticsearchMappingBuilder propNested(String fieldName, Map<String, Object> nestedProperties) {
        this.properties.put(fieldName, this.MappingTypeNested(nestedProperties));
        return this;
    }

    public MyElasticsearchMappingBuilder propDefaultFields() {
        if (!this.properties.containsKey(this.idFieldName)) {
            this.properties.put(this.idFieldName, MyElasticsearchMappingEnums._keyword);
        }

        if (!this.properties.containsKey(this.timeFieldName)) {
            this.properties.put(this.timeFieldName, MyElasticsearchMappingEnums._date);
        }

        return this;
    }

    public Map<String, Object> Build() {
        return this.properties;
    }
}
