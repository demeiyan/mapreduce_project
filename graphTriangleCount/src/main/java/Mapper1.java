import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 读取边a-b(a<b)
 * created by dmyan on 17-11-17
 */
public class Mapper1 extends Mapper<LongWritable,Text,Text,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] lines = value.toString().split("\\s+");
        String idA = lines[0];
        String idB = lines[1];
        if(idA.compareTo(idB)>0){
            String tmp = idA;
            idA = idB;
            idB = tmp;
            context.write(new Text(idA+"-"+idB),new Text("link"));
        }else if(idA.compareTo(idB)<0){
            context.write(new Text(idA+"-"+idB),new Text("link"));
        }

    }
}
