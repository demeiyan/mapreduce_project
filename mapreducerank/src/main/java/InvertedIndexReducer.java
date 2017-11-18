import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by dmyan on 17-10-26.
 */
public class InvertedIndexReducer extends Reducer<Text, IntWritable, Text, Text> {

    private static List<String> postingsList = new ArrayList<String>();
    private static Text item = new Text(" ");
    //private static Configuration hbase_conf = HBaseConfiguration.create();
    //private static HBaseAdmin admin;
/*    private void createTable(String tableName,String colFamilies[]) throws IOException {
        if(!admin.tableExists(Bytes.toBytes(tableName)))
        {
            HTableDescriptor tableDescriptor=new HTableDescriptor(tableName);
            int len = colFamilies.length;
            for(int i=0;i<len;i++){
                HColumnDescriptor columnDescriptor=new HColumnDescriptor(Bytes.toBytes(colFamilies[i]));
                tableDescriptor.addFamily(columnDescriptor);
            }

            admin.createTable(tableDescriptor);
            System.out.println("创建表成功！");

        }
    }*/
/*
    private void addData(String tableName,String rowKey,String family,String qualifier,String value) throws IOException {
*//*        Connection conn = ConnectionFactory.createConnection(hbase_conf);
        // 获取表
        HTable table = (HTable) conn.getTable(TableName.valueOf(tableName));
        //HTable table = new HTable(hbase_conf,tableName);
        //HBaseAdmin admin=new HBaseAdmin(hbase_conf);
        Put put = new Put(rowKey.getBytes());
        put.addColumn(family.getBytes(),qualifier.getBytes(),value.getBytes());
        table.put(put);*//*
        try{
            //HTable table = new HTable(hbase_conf,tableName);
            Connection conn = ConnectionFactory.createConnection(hbase_conf);
            // 获取表
            HTable table = (HTable) conn.getTable(TableName.valueOf(tableName));
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(family),Bytes.toBytes(qualifier),Bytes.toBytes(value));
            table.put(put);
        }catch (IOException e){
            e.printStackTrace();
        }
    }*/
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values,
                          Context context) throws IOException,InterruptedException {
        Text wordpre;
        Text word = new Text();
        int sum = 0;
        wordpre = new Text(key.toString().split(",")[0]);
        String fileName = key.toString().split(",")[1];
        for(IntWritable value : values){
            sum += value.get();
        }
        word.set(fileName+":"+sum);//文件名和总数
        //postingsList.add();
        if(!item.equals(wordpre)&&!item.equals(" ")){
            long frens = 0;
            double fileCount = 0.0;
            StringBuilder all = new StringBuilder();
            Iterator<String> iter = postingsList.iterator();
            String str ;
            if(iter.hasNext()){
                str = iter.next();
                all.append(str);
                fileCount++;
                frens += Long.parseLong(str.substring(str.indexOf(":")+1));
            }
            while(iter.hasNext()){
                str = iter.next();
                all.append(";");
                fileCount++;
                frens += Long.parseLong(str.substring(str.indexOf(":")+1));
                all.append(str);
            }
            if(frens>0){
                //addData("Wuxia",item.toString(),"count","freqs",String.format("%.2f",frens/fileCount)+"");
                context.write(item,new Text(String.format("%.2f",frens/fileCount)+","+all.toString()));
            }
            postingsList = new ArrayList<String>();
        }
        item.set(wordpre);
        postingsList.add(word.toString());

    }
    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        long frens = 0;
        double fileCount = 0.0;
        StringBuilder all = new StringBuilder();
        Iterator<String> iter = postingsList.iterator();
        String str ;
        if(iter.hasNext()){
            str = iter.next();
            all.append(str);
            fileCount++;
            frens += Long.parseLong(str.substring(str.indexOf(":")+1));
        }
        while(iter.hasNext()){
            str = iter.next();
            all.append(";");
            fileCount++;
            frens += Long.parseLong(str.substring(str.indexOf(":")+1));
            all.append(str);
        }
        if(frens>0){
            //addData("Wuxia",item.toString(),"count","freqs",String.format("%.2f",frens/fileCount)+"");
            context.write(item,new Text(String.format("%.2f",frens/fileCount)+","+all.toString()));
        }
    }

}
