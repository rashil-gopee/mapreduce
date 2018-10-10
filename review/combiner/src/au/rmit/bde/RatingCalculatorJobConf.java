package au.rmit.bde;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
//import org.apache.log4j.Logger;

public class RatingCalculatorJobConf extends Configured implements Tool {


    //Map Class
    static public class RatingCalculatorMapper extends Mapper<LongWritable, Text, Text, Text> {
//		   private Logger logger = Logger.getLogger(RatingCalculatorMapper.class);
        //Simple Mapper

        private Map<String, String> tokenMap;

        //
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            tokenMap = new HashMap<String, String>();
        }

        @Override
        protected void map(LongWritable offset, Text text, Context context) throws IOException, InterruptedException {
            if (offset.get() != 0) {
                String[] review = text.toString().split("\t");

                String productId = review[3];
                int rating = Integer.parseInt(review[7]);

                String result = tokenMap.get(productId);

                if (result != null) {
                    String[] tokens = result.split("\\s");
                    float count = Float.parseFloat(tokens[0]);
                    float sum = Float.parseFloat(tokens[1]);
                    count++;
                    sum += rating;
                    tokenMap.put(productId, count + " " + sum);
                } else {
                    tokenMap.put(productId, 1 + " " + rating);
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Text writableText = new Text();
            Text writableKey = new Text();
            Set<String> keys = tokenMap.keySet();
            for (String key : keys) {
                writableKey.set(key);
                writableText.set(tokenMap.get(key));
                context.write(writableKey, writableText);
            }
        }
    }

    static public class RatingCalculatorReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text token, Iterable<Text> ratings, Context context)
                throws IOException, InterruptedException {

            float totalCount = 0;
            float totalRating = 0;

            //Calculate average of ratings
            for (Text rating : ratings) {
                String[] tokens = rating.toString().split("\\s");
                if (tokens.length == 2) {
                    float count = Float.parseFloat(tokens[0]);
                    float sum = Float.parseFloat(tokens[1]);
                    totalCount = +count;
                    totalRating += sum;
                }
            }

            float average = totalRating / totalCount;

            context.write(token, new Text(String.valueOf(average)));
        }
    }

    public int run(String[] args) throws Exception {
        Configuration configuration = getConf();
//	      configuration.set("mapreduce.job.jar", "/home/hadoop/wordcount.jar");

        configuration.set("mapreduce.job.jar", args[2]);
        //Initialising Map Reduce Job
        Job job = new Job(configuration, "Rating Average");

        //Set Map Reduce main jobconf class
        job.setJarByClass(RatingCalculatorMapper.class);

        //Set Mapper class
        job.setMapperClass(RatingCalculatorMapper.class);

        //Set Combiner class
        job.setCombinerClass(RatingCalculatorReducer.class);

        //set Reducer class
        job.setReducerClass(RatingCalculatorReducer.class);


        //set Input Format
        job.setInputFormatClass(TextInputFormat.class);

        //set Output Format
        job.setOutputFormatClass(TextOutputFormat.class);

        //set Output key class
        job.setOutputKeyClass(Text.class);

        //set Output value class
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : -1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new RatingCalculatorJobConf(), args));
    }
}
