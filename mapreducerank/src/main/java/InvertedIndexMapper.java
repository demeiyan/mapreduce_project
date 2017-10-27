

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by dmyan on 17-10-26.
 */
public class InvertedIndexMapper extends Mapper<LongWritable,Text,Text,Text>{
    private Text word = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        FileSplit  fileSplit = (FileSplit)context.getInputSplit();
        String fileName = fileSplit.getPath().getName();
        StringTokenizer token = new StringTokenizer(value.toString());//字符拆解成单词
        while(token.hasMoreTokens()){
            word.set(token.nextToken()+","+fileName);
            context.write(word, new Text("1"));
        }
    }
}
