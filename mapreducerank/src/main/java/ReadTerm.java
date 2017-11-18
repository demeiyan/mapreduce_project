import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;

/**
 * Created by dmyan on 17-11-8.
 */
public class ReadTerm {
    private static Configuration hbase_conf = HBaseConfiguration.create();
    public static void main(String[] args) throws IOException {
        hbase_conf.set("hbase.zookeeper.quorum", "localhost");
        hbase_conf.set("hbase.zookeeper.property.clientPort", "2181");
        // 建立一个数据库的连接
        //Connection conn = ConnectionFactory.createConnection(hbase_conf);
        // 获取表
        //HTable table = (HTable) conn.getTable(TableName.valueOf("wuxia"));
        // 创建一个扫描对象
        //Scan scan = new Scan();
        // 扫描全表输出结果
        //ResultScanner results = table.getScanner(scan);
        File file = new File("Wuxia.txt");
        File file1 = new File("wuxia.txt");
        BufferedWriter bw = new BufferedWriter(new FileWriter(file1,true));
        BufferedReader br = new BufferedReader(new FileReader(file));
        String content ;
        String[] strs;
        Connection conn = ConnectionFactory.createConnection(hbase_conf);
        // 获取表
        HTable table = (HTable) conn.getTable(TableName.valueOf("Wuxia"));
        for(content=br.readLine();content!=null;content = br.readLine()){
            strs = content.split("\t");
            strs[1] = strs[1].substring(0,strs[1].indexOf(","));
            bw.write(strs[0]+"\t"+strs[1]);
            bw.newLine();
            addData(table,strs[0],"count","freqs",strs[1]);
            //System.out.println(content);
        }
/*
        for (Result result : results) {
            for (Cell cell : result.rawCells()) {
                bw.write(new String(CellUtil.cloneRow(cell))+"\t"+new String(CellUtil.cloneValue(cell)) );
                System.out.println(

                        "行键:" + new String(CellUtil.cloneRow(cell)) + "\t" +
                                "列族:" + new String(CellUtil.cloneFamily(cell)) + "\t" +
                                "列名:" + new String(CellUtil.cloneQualifier(cell)) + "\t" +
                                "值:" + new String(CellUtil.cloneValue(cell)) + "\t" +
                                "时间戳:" + cell.getTimestamp());
            }
            bw.newLine();
        }*/
        // 关闭资源
        //bw.close();
        //results.close();
        br.close();
        bw.close();
        //table.close();
        //conn.close();
    }


    private static void addData(HTable table,String rowKey,String family,String qualifier,String value) throws IOException {
/*        Connection conn = ConnectionFactory.createConnection(hbase_conf);
        // 获取表
        //HTable table = (HTable) conn.getTable(TableName.valueOf(tableName));
        //HTable table = new HTable(hbase_conf,tableName);
        //HBaseAdmin admin=new HBaseAdmin(hbase_conf);
        Put put = new Put(rowKey.getBytes());
        put.addColumn(family.getBytes(),qualifier.getBytes(),value.getBytes());
        table.put(put);*/
        try{
            //HTable table = new HTable(hbase_conf,tableName);

            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(family),Bytes.toBytes(qualifier),Bytes.toBytes(value));
            table.put(put);
        }catch (IOException e){
            e.printStackTrace();
        }
    }
}
