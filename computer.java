package computer;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class computer {

	public static class hdpMap extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] line = value.toString().split("\t");
			word.set(line[2]);
			context.write(word, one);
		}
	}

	public static class hdpReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

		Map<String, Integer> map = new HashMap<String, Integer>();
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable i : values)
				sum += i.get();
			String name = key.toString();
			map.put(name, sum);
		}
		
		@Override
		protected void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// 这里将map.entrySet()转换成list
			List<Map.Entry<String, Integer>> list = new LinkedList<Map.Entry<String, Integer>>(map.entrySet());
			// 通过比较器来实现排序
			Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
				// 降序排序
				@Override
				public int compare(Entry<String, Integer> arg0, Entry<String, Integer> arg1) {
					return (int) (arg1.getValue() - arg0.getValue());
				}
			});
			for (int i = 0; i < 30; i++) {
				context.write(new Text(list.get(i).getKey()), new IntWritable(list.get(i).getValue()));
				System.out.println("key="+list.get(i).getKey()+"value="+list.get(i).getValue());
			}
		}
	}
		public static void main(String[] args) throws Exception {

			Configuration conf = new Configuration();
			String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
			if (otherArgs.length != 2) {
				System.err.println("输入参数个数为：" + otherArgs.length + "，Usage: wordcount <in> <out>");
				System.exit(2);// 终止当前正在运行的java虚拟机
			}
			Job job = Job.getInstance(conf, "computer");
			job.setJarByClass(computer.class);//1
			job.setMapperClass(hdpMap.class);
//			job.setCombinerClass(hdpReduce.class);
			job.setReducerClass(hdpReduce.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
			FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
			// waitForCompletion()方法用来提交作业并等待执行完成
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		}

	}


