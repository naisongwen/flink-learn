//package org.learn.flink.connector.jdbc;
//
//import org.apache.flink.api.common.functions.RichMapFunction;
//import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
//import org.apache.flink.api.common.typeinfo.TypeInformation;
//import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSink;
//import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
//import org.apache.flink.api.java.typeutils.RowTypeInfo;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.table.api.EnvironmentSettings;
//import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
//import org.apache.flink.table.types.logical.RowType;
//import org.apache.flink.types.Row;
//import org.learn.flink.connector.jdbc.meta.DerbyDbMetadata;
//import org.learn.flink.connector.jdbc.meta.JDBCMixture;
//import org.learn.flink.connector.jdbc.meta.MysqlDbMetadata;
//
//import java.util.Arrays;
//
//import static org.apache.flink.table.types.utils.TypeConversions.fromLegacyInfoToDataType;
//
//public class JdbcSinkJob {
//    static final MysqlDbMetadata mysqlDbMetadata = new MysqlDbMetadata("airflow");
//    static final DerbyDbMetadata DERBY_EBOOKSHOP_DB = new DerbyDbMetadata("ebookshop");
//
//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        EnvironmentSettings envSettings = EnvironmentSettings.newInstance()
//                .useBlinkPlanner()
//                .inStreamingMode()
//                .build();
//        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, envSettings);
//        JDBCMixture.initSchema(mysqlDbMetadata);
//
//        DataStreamSource<Books> input = env.fromCollection(Arrays.asList(new Books(1,"Spark 实战","jack", 100.0,25), new Books(2,"Presto技术内幕","lucy", 88.0,24)));
//
//        DataStream<Row> ds = input.map(new RichMapFunction<Books, Row>() {
//            @Override
//            public Row map(Books books) {
//                return Row.of(books.getId(), books.getTitle(),books.getAuthor(),books.getPrice(),books.getQty());
//            }
//        });
//        TypeInformation<?>[] fieldTypes = new TypeInformation<?>[]{
//                BasicTypeInfo.INT_TYPE_INFO,
//                BasicTypeInfo.STRING_TYPE_INFO,
//                BasicTypeInfo.STRING_TYPE_INFO,
//                BasicTypeInfo.DOUBLE_TYPE_INFO,
//                BasicTypeInfo.INT_TYPE_INFO
//        };
//
//        RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes);
//        RowType rowType = (RowType) fromLegacyInfoToDataType(rowTypeInfo).getLogicalType();
//
//        //写入mysql
//        JDBCAppendTableSink sink = JDBCAppendTableSink.builder()
//                .setDrivername(mysqlDbMetadata.getDriverClass())
//                .setDBUrl(mysqlDbMetadata.getInitUrl())
//                .setUsername(mysqlDbMetadata.getLoginAccount())
//                .setPassword(mysqlDbMetadata.getLoginPwd())
//                .setParameterTypes(fieldTypes)
//                .setQuery("insert into books values(?,?,?,?,?)")
////                .setQuery("update student set age = ? where name = ?")
//                .build();
//
//        sink.consumeDataStream(ds);
//
//        //查询
//        JDBCInputFormat jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
//                .setDrivername(mysqlDbMetadata.getDriverClass())
//                .setDBUrl(mysqlDbMetadata.getUrl())
//                .setUsername(mysqlDbMetadata.getLoginAccount())
//                .setPassword(mysqlDbMetadata.getLoginPwd())
//                .setQuery("select * from books")
//                .setRowTypeInfo(rowTypeInfo)
//                //.setRowConverter(JDBCDialects.get(mysqlDbMetadata.getUrl()).get().getRowConverter(rowType))
//                .finish();
//        jdbcInputFormat.openInputFormat();
//        DataStreamSource<Row> input1 = env.createInput(jdbcInputFormat);
//        input1.print();
//        env.execute();
//    }
//
//    static class Books {
//        Integer id;
//        String title;
//        String author;
//        Double price;
//        Integer qty;
//
//        public Books(Integer id,String title, String author,Double price,Integer qty) {
//            this.id = id;
//            this.title = title;
//            this.author = author;
//            this.price = price;
//            this.qty = qty;
//        }
//
//        public Integer getId() {
//            return id;
//        }
//
//        public String getTitle() {
//            return title;
//        }
//
//        public String getAuthor() {
//            return author;
//        }
//
//        public Double getPrice() {
//            return price;
//        }
//
//        public Integer getQty() {
//            return qty;
//        }
//
//        @Override
//        public String toString(){
//            return String.format("%d-%s-%s-%f-%d",id,title,author,price,qty);
//        }
//    }
//}
