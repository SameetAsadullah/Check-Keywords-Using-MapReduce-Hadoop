import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.util.Scanner;

public class CheckKeyword {
    public static void main(String[] args) throws Exception {
        Scanner myObj = new Scanner(System.in);  // Create a Scanner object
        System.out.print("Enter Keyword: ");
        String keyword = myObj.nextLine();  // Read user input

        Configuration conf = new Configuration();
        conf.set("mapper.word", keyword);
        Job job = Job.getInstance(conf, "check keyword");
        job.setJarByClass(CheckKeyword.class);
        job.setJobName("Check keyword");
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.setMapperClass(CheckKeywordMapper.class);
        job.setReducerClass(CheckKeywordReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
