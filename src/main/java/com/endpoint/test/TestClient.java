package com.endpoint.test;

import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import com.endpoint.test.Sum.SumRequest;
import com.endpoint.test.Sum.SumResponse;
import com.endpoint.test.Sum.SumService;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.hbase.util.Bytes;

public class TestClient {

   //设置参数
    private static final String TABLE_NAME = "test2";
    private static final String FAMILY = "f1";
    private static final String COLUMN = "PBRL";
    private static final byte[] STRAT_KEY = Bytes.toBytes("000");
    private static final byte[] END_KEY = Bytes.toBytes("100");

    public static void main(String[] args) throws Exception {



        // 配置HBse
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "linux3,linux4,linux5");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.setLong("hbase.rpc.timeout", 600000);
        System.setProperty("hadoop.home.dir", "F:/ruanjian/hadoop-2.6.0-cdh5.14.0");

        // 建立一个连接
        Connection conn = ConnectionFactory.createConnection(conf);
        // 获取表
        HTable table = (HTable) conn.getTable(TableName.valueOf(TABLE_NAME));

        long sum = 0L;

        // 设置请求对象
        final SumRequest request = SumRequest.newBuilder().setFamily(FAMILY).setColumns(COLUMN).build();

        try {
            // 获得返回值
            Map<byte[], Long> result = table.coprocessorService(SumService.class, STRAT_KEY,  END_KEY,
                    new Batch.Call<SumService, Long>() {

                        @Override
                        public Long call(SumService service) throws IOException {
                            BlockingRpcCallback<SumResponse> rpcCallback = new BlockingRpcCallback<SumResponse>();
                            service.getSum(null, request, rpcCallback);
                            SumResponse response = (SumResponse) rpcCallback.get();
                            return response.hasSum() ? response.getSum() : 0L;
                        }
                    });
            // 将返回值进行迭代相加
            for (Long v : result.values()) {
                sum += v;
            }
            // 结果输出
            System.out.println("sum: " + sum);

        } catch (ServiceException e) {
            e.printStackTrace();
        }catch (Throwable e) {
            e.printStackTrace();
        }
        table.close();
        conn.close();

    }

}