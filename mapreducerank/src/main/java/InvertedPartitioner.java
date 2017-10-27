import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

/**
 * Created by dmyan on 17-10-27.
 */
public class InvertedPartitioner extends HashPartitioner<Text,IntWritable> {

    @Override
    public int getPartition(Text key, IntWritable value, int numReduceTasks) {
        Text term = new Text(key.toString().split(",")[0]);
        return super.getPartition(term,value,numReduceTasks);
    }
}
