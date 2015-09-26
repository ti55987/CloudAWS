import java.io.IOException;
import java.util.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.StringUtils;

public class NGram {
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>
	{
		private final static IntWritable one = new IntWritable(1);
		private Text out = new Text();
		
		public void map(LongWritable key, Text value, Context context) throws 
		IOException, InterruptedException{
			String line = value.toString();
			String[] words = line.replaceAll("[^a-zA-Z ]", " ").toLowerCase().split("\\s+");
			String[] copyTo ;
			for(int i = 0; i < words.length ; i++){
				if(words[i].isEmpty())
					continue;
				
				for(int j = 1; j < 6 ; j++){
					if (i+j > words.length)
						continue;
					copyTo = (String[]) java.util.Arrays.copyOfRange(words, i, i+j);
					out.set(StringUtils.join(" " , copyTo));
					context.write(out, one);
				}	
			}
		}
	}
	
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
		throws IOException, InterruptedException{
			int sum = 0;
			for(IntWritable val : values){
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}
	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		Configuration conf  = new Configuration();
		
		Job job = new Job(conf,"NGram");
		job.setJarByClass(NGram.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);

	}

}
