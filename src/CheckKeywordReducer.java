import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CheckKeywordReducer
        extends Reducer<Text, IntWritable, Text, IntWritable> {
    private int positive = 0;   // stores total positive comments
    private int negative = 0;   // stores total negative comments
    private int neutral = 0;    // stores total neutral comments
    private String keyword;     // stores keyword to search

    public void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {
        Iterator i$ = values.iterator();

        while(i$.hasNext()) {
            IntWritable value = (IntWritable)i$.next();
            if (value.get() > 0) {  // if it is a positive comment
                ++this.positive;
            } else if (value.get() < 0){    // if it is a negative comment
                ++this.negative;
            } else {    // if it is a neutral comment
                ++this.neutral;
            }
        }
    }

    protected void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {
        Configuration config = context.getConfiguration();
        keyword = config.get("mapper.word");    // reading keyword

        context.write(new Text("Total Comments"), new IntWritable(this.positive + this.negative + this.neutral));
        context.write(new Text("Positive Comments"), new IntWritable(this.positive));
        context.write(new Text("Negative Comments"), new IntWritable(this.negative));
        context.write(new Text("Neutral Comments"), new IntWritable(this.neutral));

        System.out.println("\n\nName: Sameet Asadullah\nRoll Number: 18i-0479");
        if (this.positive > this.negative && this.positive > this.neutral) {    // if overall keyword is positive
            context.write(new Text("Keyword: " + this.keyword + " used positively overall."), null);
            System.out.println("Keyword: " + this.keyword + " used positively overall.");

        } else if (this.positive < this.negative && this.neutral < this.negative){  // if overall keyword is negative
            context.write(new Text("Keyword: " + this.keyword + " used negatively overall."), null);
            System.out.println("Keyword: " + this.keyword + " used negatively overall.");
        } else {    // if overall keyword is neutral
            context.write(new Text("Keyword: " + this.keyword + " used neutrally overall."), null);
            System.out.println("Keyword: " + this.keyword + " used neutrally overall.");
        }
        System.out.println("\n");
    }
}
