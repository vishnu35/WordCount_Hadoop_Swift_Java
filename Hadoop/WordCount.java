import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Job;

public class WordCount {

	public static class Map_Count extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable intwritable = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			String temp;
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				temp = tokenizer.nextToken();
				temp = temp.replaceAll("[^a-zA-Z0-9\\s]", "");
				word.set(temp);
				output.collect(word, intwritable);
			}
		}
	}

	public static class Reduce_Count extends MapReduceBase implements
			Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {

			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}
	}

	public static class Map_sort extends MapReduceBase implements
			Mapper<Object, Text, IntWritable, Text> {

		public void map(Object key, Text value,
				OutputCollector<IntWritable, Text> collector, Reporter arg3)
				throws IOException {
			String line = value.toString();
			StringTokenizer stringTokenizer = new StringTokenizer(line);
			{
				int number = 0;
				String word = null;

				if (stringTokenizer.hasMoreTokens()) {
					String str0 = stringTokenizer.nextToken();
					word = str0.trim();
				}

				if (stringTokenizer.hasMoreElements()) {
					String str1 = stringTokenizer.nextToken();
					number = Integer.parseInt(str1.trim());
				}
				collector.collect(new IntWritable(number), new Text(word));
			}

		}

	}

	public static class Reduce_sort extends MapReduceBase implements
			Reducer<IntWritable, Text, IntWritable, Text> {

		public void reduce(IntWritable key, Iterator<Text> values,
				OutputCollector<IntWritable, Text> arg2, Reporter arg3)
				throws IOException {
			while ((values.hasNext())) {
				arg2.collect(key, values.next());
			}
		}

	}

	public static void main(String[] args) throws Exception {
		JobConf conf_count = new JobConf(WordCount.class);
		conf_count.setJobName("WordCount");

		conf_count.setOutputKeyClass(Text.class);
		conf_count.setOutputValueClass(IntWritable.class);

		conf_count.setMapperClass(Map_Count.class);
		conf_count.setCombinerClass(Reduce_Count.class);
		conf_count.setReducerClass(Reduce_Count.class);

		conf_count.setInputFormat(TextInputFormat.class);
		conf_count.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf_count, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf_count, new Path("/tmp/temp"));

		Job job_count = new Job(conf_count);
		job_count.submit();

		JobConf conf_sort = new JobConf(WordCount.class);
		conf_sort.setJobName("WordSort");

		conf_sort.setOutputKeyClass(IntWritable.class);
		conf_sort.setOutputValueClass(Text.class);

		conf_sort.setMapperClass(Map_sort.class);
		conf_sort.setCombinerClass(Reduce_sort.class);
		conf_sort.setReducerClass(Reduce_sort.class);

		conf_sort.setInputFormat(TextInputFormat.class);
		conf_sort.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf_sort, new Path("/tmp/temp/part-00000"));
		FileOutputFormat.setOutputPath(conf_sort, new Path(args[1]));

		Job job_sort = new Job(conf_sort);
		if (job_count.waitForCompletion(true)) {
			job_sort.submit();
			job_sort.waitForCompletion(true);
		}

	}
}
