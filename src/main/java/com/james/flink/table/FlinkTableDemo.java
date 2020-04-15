package com.james.flink.table;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

import java.util.ArrayList;
import java.util.List;

public class FlinkTableDemo {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);

        List list = new ArrayList();
//        String wordsStr = "Hello Flink Hello James";
//        String[] words = wordsStr.split("\\W+");
//        for (String word : words) {
//            WC wc = new WC(word, 1);
//            list.add(wc);
//        }
//        DataSet<WC> input = env.fromCollection(list);
        String filePath = "/Users/qjiang/workspace/JamesFlink/src/main/resources/data/wc.csv";
        DataSet<WC> input = env.readTextFile(filePath).map(new MapFunction<String, WC>() {
            @Override
            public WC map(String s) throws Exception {
                String[] ss = s.split("\\W+");
                WC wc = new WC(ss[0], Integer.parseInt(ss[1]));
                return wc;
            }
        });
        tEnv.createTemporaryView("WordCount", input, "word, frequency");
        Table table = tEnv.sqlQuery(
                "SELECT word, SUM(frequency) as frequency FROM WordCount GROUP BY word");
        DataSet<WC> result = tEnv.toDataSet(table, WC.class);
        result.print();
    }

    public static class WC {
        public String word;//hello
        public long frequency;//1

        // public constructor to make it a Flink POJO
        public WC() {
        }

        public WC(String word, long frequency) {
            this.word = word;
            this.frequency = frequency;
        }

        @Override
        public String toString() {
            return "WC " + word + " " + frequency;
        }
    }
}
