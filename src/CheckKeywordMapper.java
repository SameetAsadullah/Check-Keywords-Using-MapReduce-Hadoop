import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Scanner;
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CheckKeywordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Vector<String> positiveWords = new Vector();    // stores words in positive.txt
    private Vector<String> negativeWords = new Vector();    // stores words in negative.txt
    private String keyword;     // stores keyword to search

    // function to read positive.txt and negative.txt
    private void initializeArrays() {
        File myObj;
        Scanner myReader;
        String data;
        // reading positive.txt
        try {
            myObj = new File("./hadoopMR/input/positive.txt");
            myReader = new Scanner(myObj);

            while(myReader.hasNextLine()) {
                data = myReader.nextLine();
                this.positiveWords.add(data);
            }

            myReader.close();
        } catch (FileNotFoundException var5) {
            System.out.println("An error occurred.");
            var5.printStackTrace();
        }

        // reading negative.txt
        try {
            myObj = new File("./hadoopMR/input/negative.txt");
            myReader = new Scanner(myObj);

            while(myReader.hasNextLine()) {
                data = myReader.nextLine();
                this.negativeWords.add(data);
            }

            myReader.close();
        } catch (FileNotFoundException var4) {
            System.out.println("An error occurred.");
            var4.printStackTrace();
        }

    }

    public void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {
        if (this.positiveWords.isEmpty() && this.negativeWords.isEmpty()) {     // if variables are not yet initialised
            Configuration config = context.getConfiguration();
            keyword = config.get("mapper.word");
            this.initializeArrays();
        }

        String line = value.toString();
        if (line.charAt(0) != '<') {
            int starting_index = line.indexOf("Text=") + 6;
            int ending_index = line.indexOf("CreationDate=") - 2;
            String comment = line.substring(starting_index, ending_index);
            if (comment.contains(this.keyword)) {
                int count = 0;
                StringTokenizer itr = new StringTokenizer(comment);

                while(itr.hasMoreTokens()) {
                    String word = itr.nextToken();
                    if (this.positiveWords.contains(word)) {    // if word is positive
                        ++count;
                    }

                    if (this.negativeWords.contains(word)) {    // if word is negative
                        --count;
                    }
                }

                System.out.printf("Comment: %s\n", comment);
                System.out.print("Comment Sentiment: ");
                if (count > 0) {    // if overall comment is positive
                    System.out.println("Positive as most words are found in the bag of positive words.");
                } else if (count < 0){  // if overall comment is negative
                    System.out.println("Negative as most words are found in the bag of negative words.");
                } else {    // if overall comment is neutral
                    System.out.println("Neutral as both type of words are equal.");
                }

                context.write(new Text(comment), new IntWritable(count));
            }
        }

    }
}
