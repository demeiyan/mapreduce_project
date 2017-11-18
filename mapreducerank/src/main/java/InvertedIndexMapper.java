

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by dmyan on 17-10-26.
 */
public class InvertedIndexMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
    private Text word = new Text();
    private final IntWritable one = new IntWritable(1);
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        FileSplit  fileSplit = (FileSplit)context.getInputSplit();
        String fileName = fileSplit.getPath().getName();
        //int index;
        if(fileName.contains(".TXT.segmented"))
        fileName = fileName.replace(".TXT.segmented","");
        if(fileName.contains(".txt.segmented")) fileName = fileName.replace(".txt.segmented","");
 /*       if(fileName.contains(".")){
            index = fileName.indexOf(".");
        }else {
            index = fileName.length();
        }
        fileName = fileName.substring(0,index);*/
        StringTokenizer token = new StringTokenizer(value.toString());//字符拆解成单词
        while(token.hasMoreTokens()){
            word.set(token.nextToken()+","+fileName);
            context.write(word, one);
        }
    }
}
