import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * created by dmyan on 17-11-17
 */
public class TriangleCount {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job1 = Job.getInstance();
        job1.setJarByClass(TriangleCount.class);
        job1.setMapperClass(Mapper1.class);
        job1.setReducerClass(Reducer1.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job1,new Path(args[0]));
        //FileInputFormat.addInputPath(job1,new Path(args[0]+"/tmp/step2/part-r-00000"));
        FileOutputFormat.setOutputPath(job1,new Path(args[1]+"/tmp/step1"));
        //FileOutputFormat.setOutputPath(job1,new Path(args[1]));
        job1.waitForCompletion(true);
        //System.exit(job1.waitForCompletion(true)?0:1);

        Job job2 = Job.getInstance();
        job2.setJarByClass(TriangleCount.class);
        job2.setMapperClass(Mapper2.class);
        job2.setReducerClass(Reducer2.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2,new Path(args[1]+"/tmp/step1/part-r-00000"));
        FileOutputFormat.setOutputPath(job2,new Path(args[1]+"/tmp/step2"));
        job2.waitForCompletion(job1.isComplete());

        Job job3 = Job.getInstance();
        job3.setJarByClass(TriangleCount.class);
        job3.setMapperClass(Mapper3.class);
        job3.setReducerClass(Reducer3.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job3,new Path(args[1]+"/tmp/step2/part-r-00000"));
        FileOutputFormat.setOutputPath(job3,new Path("gplus_resultOutput"));
        job3.waitForCompletion(job2.isComplete());
    }
}
