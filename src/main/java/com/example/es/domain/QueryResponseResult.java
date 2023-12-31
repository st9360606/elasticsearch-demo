package com.example.es.domain;

import lombok.Data;
import lombok.ToString;

/**
 * creste by itheima.itcast
 */
@Data
@ToString
public class QueryResponseResult<T> extends ResponseResult {

    QueryResult<T> queryResult;

    public QueryResponseResult(ResultCode resultCode,QueryResult queryResult){
        super(resultCode);
        this.queryResult = queryResult;
    }

}
