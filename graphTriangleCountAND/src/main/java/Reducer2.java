import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * created by dmyan on 17-11-21
 */
public class Reducer2 extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Text edge = new Text();
        Text rel = new Text();
        for(Text value:values){
            edge.set(key.toString()+"-"+value.toString());
            rel.set("link");
            context.write(edge,rel);
            edge.set(value.toString()+"-"+key.toString());
            rel.set("search");
            context.write(edge,rel);
            context.progress();
        }
    }
}
