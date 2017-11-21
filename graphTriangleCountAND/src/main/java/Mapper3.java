import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * created by dmyan on 17-11-21
 */
public class Mapper3 extends Mapper<LongWritable,Text,Text,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] line = value.toString().split("\t");
        if(line.length >=2)
            context.write(new Text(line[0].trim()),new Text(line[1].trim()));

    }
}
