import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 去除重复边
 * created by dmyan on 17-11-17
 */
public class Reducer1  extends Reducer<Text, Text, Text, Text> {
    private Text value = new Text();
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        value.set("");
        context.write(key,value);
    }
}
