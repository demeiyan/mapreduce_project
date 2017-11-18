import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * created by dmyan on 17-11-17
 */
public class Mapper2 extends Mapper<LongWritable,Text,Text,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Text idA = new Text() ;
        idA.set(value.toString().split("-")[0]);
        Text idB = new Text(value.toString().split("-")[1].trim());
        context.write(idA,idB);
    }
}
