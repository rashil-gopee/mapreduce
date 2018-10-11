package au.rmit.bde;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class RatingCalculatorJobConf extends Configured implements Tool {
    private  static  Logger LOG = Logger.getLogger(RatingCalculatorMapper.class);
    //Map Class
    static public class RatingCalculatorMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        private Map<String, Long> tokenMap;
        private Logger logger = Logger.getLogger(RatingCalculatorMapper.class);

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            LOG.setLevel(Level.INFO);
            LOG.info("Mapper Started");
            tokenMap = new HashMap<String, Long>();
        }

        @Override
        protected void map(LongWritable offset, Text text, Context context) throws IOException, InterruptedException {
            LOG.setLevel(Level.DEBUG);
            //ignore header line
            if (offset.get() != 0) {
                String[] review = text.toString().split("\t");

                String productId = review[3];
                char confirmedPurchase = review[11].charAt(0);

                if (confirmedPurchase == 'Y') {
                    if (tokenMap.containsKey(productId)) {
                        long total = tokenMap.get(productId);
                        total++;
                        tokenMap.put(productId, total);
                    } else {
                        tokenMap.put(productId, 1L);
                    }
                } else {
                    if (!tokenMap.containsKey(productId)) {
                        tokenMap.put(productId, 0L);
                    }
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            LOG.setLevel(Level.DEBUG);
            LongWritable writableCount = new LongWritable();
            Text token = new Text();
            Set<String> keys = tokenMap.keySet();
            for (String key : keys) {
                token.set(key);
                writableCount.set(tokenMap.get(key));
                context.write(token, writableCount);
            }
        }
    }

    static public class RatingCalculatorReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        private LongWritable total = new LongWritable();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            LOG.setLevel(Level.INFO);
            LOG.info("Reducer Started");
        }

        @Override
        protected void reduce(Text token, Iterable<LongWritable> counts, Context context)
                throws IOException, InterruptedException {
            LOG.setLevel(Level.DEBUG);

            long n = 0;
            //Calculate sum of counts
            for (LongWritable count : counts)
                n += count.get();
            total.set(n);

            context.write(token, total);
        }
    }


    //Partitions the mappers result into 4 depending on the length of the key and the type of it's first character
    //Thus same key-pair values would go to the same reducer
//    public static class RatingCalculatorPartioner extends Partitioner<Text, LongWritable> {
//        public int getPartition(Text key, LongWritable value, int numReduceTasks) {
//            if (numReduceTasks == 0)
//                return 0;
//            if (key.getLength() <= 9) {
//                if (Character.isDigit(key.toString().charAt(0))) {
//                    return 0 % numReduceTasks;
//                } else {
//                    return 1 % numReduceTasks;
//                }
//            }
//            else {
//                if (Character.isDigit(key.toString().charAt(0))) {
//                    return 2 % numReduceTasks;
//                } else {
//                    return 3 % numReduceTasks;
//                }
//            }
//        }
//    }

    public int run(String[] args) throws Exception {
        LOG.setLevel(Level.INFO);

        Configuration configuration = getConf();

        configuration.set("mapreduce.job.jar", args[2]);
        //Initialising Map Reduce Job
        Job job = new Job(configuration, "In-Mapper Combiner)");

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
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

//        job.setPartitionerClass(RatingCalculatorPartioner.class);
//        job.setNumReduceTasks(4);

        return job.waitForCompletion(true) ? 0 : -1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new RatingCalculatorJobConf(), args));
    }
}
