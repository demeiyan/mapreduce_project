import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * 把边和关系给reduce
 * created by dmyan on 17-11-17
 */
public class Mapper3 extends Mapper<LongWritable,Text,Text,Text>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if(value.toString().trim().length()>0){
            String[] line = value.toString().split("\t");
            if(line.length >=2)
            context.write(new Text(line[0].trim()),new Text(line[1].trim()));
        }
    }
}
