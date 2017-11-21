import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * created by dmyan on 17-11-21
 */
public class Mapper4 extends Mapper<LongWritable,Text,Text,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split("\\s+");
        if(line[0].compareTo(line[1])<0){
            context.write(new Text(line[0]),new Text(line[1]));
        }
    }
}
