package com.chaolj.core.elasticsearchProvider;

public enum MyElasticsearchMappingEnums {
    _text("text", 0),
    _keyword("keyword", 1),
    _byte("byte", 2),
    _short("short", 3),
    _integer("integer", 4),
    _long("long", 5),
    _float("float", 6),
    _double("double", 7),
    _boolean("boolean", 8),
    _date("date", 9);

    private String name;
    private Integer value;

    private MyElasticsearchMappingEnums(String name, Integer value){
        this.name=name;
        this.value=value;
    }

    public String getName(){
        return this.name;
    }

    public Integer getValue(){
        return this.value;
    }

    public static MyElasticsearchMappingEnums valueOf(Integer value) {
        MyElasticsearchMappingEnums result = null;
        for (var item : MyElasticsearchMappingEnums.values()){
            if (item.value.equals(value)) {
                result = item;
                break;
            }
        }

        if (result == null) throw new RuntimeException("值（" + value + "），不在枚举（ElasticsearchPropEnums）范围内！");

        return result;
    }
}
