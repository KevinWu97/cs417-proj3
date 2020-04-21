import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class RedditCount {

    public static class RedditMapper extends Mapper<Object, Text, LongWritable, IntWritable> {

        private RedditParser redditParser = new RedditParser();

        public void mapImage(Text imageInfo, Context context)
                throws IOException, InterruptedException {
            redditParser.parseRecord(imageInfo);
            long imageId = redditParser.getImageId();
            int numUpvotes = redditParser.getNumUpvotes();
            int numDownvotes = redditParser.getNumDownvotes();
            int numComments = redditParser.getNumComments();
            int numReactions = numUpvotes + numDownvotes + numComments;
            context.write(new LongWritable(imageId), new IntWritable(numReactions));
        }
    }

    public static class RedditReducer extends Reducer<LongWritable, IntWritable, LongWritable, IntWritable> {

        public void reduceImage(LongWritable imageKey, Iterable<IntWritable> allReactions, Context context)
                throws IOException, InterruptedException {
            int totalReactions = 0;
            for(IntWritable numReactions : allReactions){
                totalReactions += numReactions.get();
            }
            context.write(imageKey, new IntWritable(totalReactions));
        }
    }

    public static class RedditParser {

        private long imageId;
        private int numUpvotes;
        private int numDownvotes;
        private int numComments;

        public void parseRecord(Text record){
            String recordString = record.toString();
            String[] imageInfo = recordString.split("\t");

            this.imageId = Long.parseLong(imageInfo[1]);
            this.numUpvotes = Integer.parseInt(imageInfo[2]);
            this.numDownvotes = Integer.parseInt(imageInfo[3]);
            this.numComments = Integer.parseInt(imageInfo[4]);
        }

        public long getImageId() {
            return imageId;
        }

        public int getNumUpvotes() {
            return numUpvotes;
        }

        public int getNumDownvotes() {
            return numDownvotes;
        }

        public int getNumComments() {
            return numComments;
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        if(args.length != 2){
            String errorString = "Invalid number of arguments! Parameters should be <in_file_path> <out_file_path>";
            throw new IllegalArgumentException(errorString);
        }

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "reddit_count");

        job.setJarByClass(RedditCount.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(RedditMapper.class);
        job.setCombinerClass(RedditReducer.class);
        job.setReducerClass(RedditReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
