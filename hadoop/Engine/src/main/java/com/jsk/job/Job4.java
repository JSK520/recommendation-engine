package com.jsk.job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Job4 extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Job4(), args);
	}
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		String inpath = conf.get("inpath");
		String outpath = conf.get("outpath");
		//构建job
		Job job = Job.getInstance(conf);
		
		//指定一个完整MapRed所在类以及相关信息
		job.setJarByClass(Job4.class);
		job.setJobName("Job4");
		//指定Map程序所在的类以及keyout valueout类型
		job.setMapperClass(Simliary2Mapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		//指定reduce程序所在的类以及keyout valueout类型IntWritable
		job.setReducerClass(Similary2Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(List.class);
		//判断一下输入的路径是否存在  存在就先删除
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path(outpath)))
			fs.delete(new Path(outpath), true);
		
		//指定数据来源和数据去向相关的输入输出流以及文件位置
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		TextInputFormat.addInputPath(job, new Path(inpath));
		TextOutputFormat.setOutputPath(job, new Path(outpath));
		//提交运行任务
		
		return job.waitForCompletion(true)?0:1;
	}
	static class Simliary2Mapper extends Mapper<LongWritable, Text, Text, Text>{
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
//			20001		20001		3  
			
			String[] split = value.toString().split("\t");
			context.write(new Text(split[0]), new Text(split[1]+":"+split[2]));
		}
	}
	static class Similary2Reducer extends Reducer<Text, Text, Text, List<Text>>{
		@Override
		protected void reduce(Text key, Iterable<Text> value, Reducer<Text, Text, Text, List<Text>>.Context context)
				throws IOException, InterruptedException {
			
			List  list = new ArrayList<>();
			for (Text text : value) {
				list.add(text.toString());
			}
			context.write(key, list);
		}
	}
}

