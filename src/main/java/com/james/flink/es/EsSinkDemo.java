package com.james.flink.es;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class EsSinkDemo {
    public static void main(String[] args) throws Exception {
        HttpHost httpHost = new HttpHost("localhost", 9200, "http");
        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(httpHost);
        ElasticsearchSink.Builder<Tuple2<String, Integer>> esSinkBuilder = new ElasticsearchSink.Builder<Tuple2<String, Integer>>(httpHosts,
                new ElasticsearchSinkFunction<Tuple2<String, Integer>>() {

                    public UpdateRequest updateIndexRequest(Tuple2<String, Integer> element) throws IOException {
                        String id = element.f0;
                        Integer status = element.f1;
                        UpdateRequest updateRequest = new UpdateRequest();
                        //设置表的index和type,必须设置id才能update
                        updateRequest.index("trafficwisdom.test_index").type("route").id(id).doc(XContentFactory.jsonBuilder().startObject().field("status", status).endObject());
                        return updateRequest;
                    }

                    @Override
                    public void process(Tuple2<String, Integer> element, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                        try {
                            requestIndexer.add(updateIndexRequest(element));
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
        );
    }
}
