package com.example.es;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = SearchApplication.class)
public class TestDocument {

    @Autowired
    RestHighLevelClient client;

    @Test
    public void testGet() throws IOException {
        GetRequest getRequest = new GetRequest("test_post", "1");

        //========================可选参数 start======================
        //为特定字段配置_source_include
//        String[] includes = new String[]{"user", "message"};
//        String[] excludes = Strings.EMPTY_ARRAY;
//        FetchSourceContext fetchSourceContext = new FetchSourceContext(true, includes, excludes);
//        getRequest.fetchSourceContext(fetchSourceContext);

        //为特定字段配置_source_excludes
//        String[] includes1 = Strings.EMPTY_ARRAY;
//        String[] excludes1 = new String[]{"user", "message"};
//        FetchSourceContext fetchSourceContext1 = new FetchSourceContext(true, includes1, excludes1);
//        getRequest.fetchSourceContext(fetchSourceContext1);

        //设置路由
//        getRequest.routing("routing");

        //查询 同步查询
        GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);

        //异步查询
//        ActionListener<GetResponse> listener = new ActionListener<GetResponse>() {
//            @Override
//            public void onResponse(GetResponse getResponse) {
//                System.out.println(getResponse.getId());
//                System.out.println(getResponse.getVersion());
//                System.out.println(getResponse.getSourceAsString());
//            }
//
//            @Override
//            public void onFailure(Exception e) {
//                e.printStackTrace();
//            }
//        };
//
//        //异步請求
//        client.getAsync(getRequest, RequestOptions.DEFAULT, listener);
//
//        try {
//            Thread.sleep(5000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }

        if (getResponse.isExists()) {
//        System.out.println(getResponse.getId());
//        System.out.println(getResponse.getVersion());
//        System.out.println(getResponse.getSourceAsString());
//        System.out.println(getResponse.getSourceAsBytes());
//        System.out.println(getResponse.getSourceAsMap());
            Map<String, Object> sourceAsMap = getResponse.getSourceAsMap();
            System.out.println(sourceAsMap.get("user"));
        }
    }


    @Test
    public void testAdd() throws IOException {
        //創建請求
        IndexRequest request = new IndexRequest("test_post");
        request.id("5");
//        =======================构建文档============================
//        构建方法 1
//        String jsonString = "{\n" +
//                "  \"user\":\"tomas J\",\n" +
//                "  \"postDate\":\"2019-07-18\",\n" +
//                "  \"message\":\"trying out es3\"\n" +
//                "}";
//        request.source(jsonString, XContentType.JSON);

        //构建方法 2
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("user", "tomas");
        jsonMap.put("postDate", "2019-07-18");
        jsonMap.put("message", "trying out es2");
        request.source(jsonMap);

////        构建方法 3
//        XContentBuilder builder = XContentFactory.jsonBuilder();
//        builder.startObject();
//        {
//            builder.field("user", "tomas");
//            builder.timeField("postDate", new Date());
//            builder.field("message", "trying out es2");
//        }
//        builder.endObject();
//        request.source(builder);
//
////        构建方法 4
//        request.source("user", "tomas",
//                "postDate", new Date(),
//                "message", "trying out es2");

        //        ========================可选参数===================================
        //设置超时时间
//        request.timeout(TimeValue.timeValueSeconds(1));
//        request.timeout("1s");

        //手動维护版本号
//        request.version(4);
//        request.versionType(VersionType.EXTERNAL);


//       2、执行
        //同步
        IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);

        //异步
//        ActionListener<IndexResponse> listener=new ActionListener<IndexResponse>() {
//            //成功時
//            @Override
//            public void onResponse(IndexResponse indexResponse) {
//                System.out.println(indexResponse.getIndex());
//                System.out.println(indexResponse.getId());
//                System.out.println(indexResponse.getResult());
//            }
//            //失敗時
//            @Override
//            public void onFailure(Exception e) {
//
//            }
//        };
//        client.indexAsync(request,RequestOptions.DEFAULT, listener );
//        try {
//            Thread.sleep(5000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }


//        3、获取结果
        System.out.println(indexResponse.getIndex());
        System.out.println(indexResponse.getId());
        System.out.println(indexResponse.getResult());

        String index = indexResponse.getIndex();
        String id = indexResponse.getId();
        //获取插入的类型
        if (indexResponse.getResult() == DocWriteResponse.Result.CREATED) {
            DocWriteResponse.Result result = indexResponse.getResult();
            System.out.println("CREATED:" + result);
        } else if (indexResponse.getResult() == DocWriteResponse.Result.UPDATED) {
            DocWriteResponse.Result result = indexResponse.getResult();
            System.out.println("UPDATED:" + result);
        }

        //獲取分片
        ReplicationResponse.ShardInfo shardInfo = indexResponse.getShardInfo();
        if (shardInfo.getTotal() != shardInfo.getSuccessful()) {
            System.out.println("处理成功的分片数少于总分片！");
        }
        if (shardInfo.getFailed() > 0) {  //失敗分片 > 0
            for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
                String reason = failure.reason();//每一個錯誤原因
                System.out.println(reason);
            }
        }

    }


    @Test
    public void testUpdate() throws IOException {
        //1、創建請求
        UpdateRequest request = new UpdateRequest("test_post", "3");
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("user", "tomas JJ");
        request.doc(jsonMap); // 注意: 是doc
        //===============================可选参数==========================================
        request.timeout("1s");//超时时间

        //重试次数
        request.retryOnConflict(3);

        //设置在继续更新之前，必须激活的分片数   不常用
//        request.waitForActiveShards(2);
        //所有分片都是active状态，才更新       不常用
//        request.waitForActiveShards(ActiveShardCount.ALL);

        //2、执行
//        同步
        UpdateResponse updateResponse = client.update(request, RequestOptions.DEFAULT);

//        異步
//        ActionListener<UpdateResponse> listener = new ActionListener<UpdateResponse>() {
//            //成功時
//            @Override
//            public void onResponse(UpdateResponse updateResponse) {
//                System.out.println(updateResponse.getIndex());
//                System.out.println(updateResponse.getId());
//                System.out.println(updateResponse.getResult());
//            }
//
//            //失敗時
//            @Override
//            public void onFailure(Exception e) {
//
//            }
//        };
//        client.updateAsync(request, RequestOptions.DEFAULT,listener);
//        try {
//            Thread.sleep(5000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }


        //3、获取数据
        updateResponse.getId();
        updateResponse.getIndex();

        //判断结果
        if (updateResponse.getResult() == DocWriteResponse.Result.CREATED) {
            DocWriteResponse.Result result = updateResponse.getResult();
            System.out.println("CREATED:" + result);
        } else if (updateResponse.getResult() == DocWriteResponse.Result.UPDATED) {
            DocWriteResponse.Result result = updateResponse.getResult();
            System.out.println("UPDATED:" + result);
        } else if (updateResponse.getResult() == DocWriteResponse.Result.DELETED) {
            DocWriteResponse.Result result = updateResponse.getResult();
            System.out.println("DELETED:" + result);
        } else if (updateResponse.getResult() == DocWriteResponse.Result.NOOP) {
            //没有操作
            DocWriteResponse.Result result = updateResponse.getResult();
            System.out.println("NOOP:" + result);
        }

    }


    @Test
    public void testDelete() throws IOException {
//        1、构建请求
        DeleteRequest request =new DeleteRequest("test_post","3");
        //可选参数
//        request.timeout("1s");//超时时间


//        2、执行
        //同步 也可以使用異步
        DeleteResponse deleteResponse = client.delete(request, RequestOptions.DEFAULT);

        //異步
//        ActionListener<DeleteResponse> listener = new ActionListener<DeleteResponse>() {
//            //成功時
//            @Override
//            public void onResponse(DeleteResponse deleteResponse) {
//                System.out.println(deleteResponse.getIndex());
//                System.out.println(deleteResponse.getId());
//                System.out.println(deleteResponse.getResult());
//            }
//
//            //失敗時
//            @Override
//            public void onFailure(Exception e) {
//
//            }
//        };
//        client.deleteAsync(request, RequestOptions.DEFAULT,listener);
//        try {
//            Thread.sleep(5000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }

//        3、获取数据
        deleteResponse.getId();
        deleteResponse.getIndex();

        DocWriteResponse.Result result = deleteResponse.getResult();
        System.out.println(result);
    }


    @Test
    public void testBulk() throws IOException {
        //1、创建请求
        BulkRequest request = new BulkRequest();
        //新增2個數據
//        request.add(new IndexRequest("post").id("1").source(XContentType.JSON, "field", "1"));
//        request.add(new IndexRequest("post").id("2").source(XContentType.JSON, "field", "2"));

        request.add(new UpdateRequest("post","1").doc(XContentType.JSON, "field", "3"));
        request.add(new DeleteRequest("post").id("2"));

        //2、执行
        BulkResponse bulkResponse = client.bulk(request, RequestOptions.DEFAULT);

        //3、獲取結果
        for (BulkItemResponse itemResponse : bulkResponse) {
            DocWriteResponse itemResponseResponse = itemResponse.getResponse();

            switch (itemResponse.getOpType()) {
                case INDEX:
                case CREATE:
                    IndexResponse indexResponse = (IndexResponse) itemResponseResponse;
                    System.out.println(indexResponse.getResult());
                    break;
                case UPDATE:
                    UpdateResponse updateResponse = (UpdateResponse) itemResponseResponse;
                    System.out.println(updateResponse.getResult());
                    break;
                case DELETE:
                    DeleteResponse deleteResponse = (DeleteResponse) itemResponseResponse;
                    System.out.println(deleteResponse.getResult());
                    break;
            }
        }
    }

}
