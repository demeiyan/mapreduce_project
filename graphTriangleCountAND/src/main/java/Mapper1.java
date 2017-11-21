import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * created by dmyan on 17-11-17
 */
public class Mapper1  extends  Mapper<LongWritable,Text,Text,Text> {
@Override
protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] lines = value.toString().split("\\s+");
        String idA = lines[0];
        String idB = lines[1];
        context.write(new Text(idA+"-"+idB),new Text(""));
        }
}
