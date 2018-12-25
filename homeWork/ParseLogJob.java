package com.bigdata.etl.job;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.bigdata.etl.mr.*;
import com.bigdata.etl.utils.IPUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Map;

public class ParseLogJob extends Configured implements Tool {

    public static LogGenericWritable parseLog(String row) throws Exception {
        String[] logPart = StringUtils.split(row,"\u1111");
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

        long timeTag = dateFormat.parse(logPart[0]).getTime();
        String activeName = logPart[1];
        JSONObject bizData = JSON.parseObject(logPart[2]);

        LogGenericWritable logData = new LogWritable();
        logData.put("time_tag", new LogFieldWritable(timeTag));
        logData.put("active_name", new LogFieldWritable(activeName));

        for(Map.Entry<String, Object> entry: bizData.entrySet()){
            logData.put(entry.getKey(), new LogFieldWritable(entry.getValue()));
        }


        return logData;



    }

    public static class LogWritable extends LogGenericWritable{

        @Override
        protected String[] getFieldName() {
            return new String[]{"active_name", "session_id", "time_tag", "ip", "device_id", "req_url", "user_id", "product_id", "order_id", "error_flag", "error_log"};


        }
    }



    public static class LogMapper extends Mapper<LongWritable, Text, TextLongWritable, LogGenericWritable>{
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{



            try {
                LogGenericWritable parsedLog = parseLog(value.toString());


                String session = (String)parsedLog.getObject("session_id");
                Long timeTag = (Long) parsedLog.getObject("time_tag");

                TextLongWritable outKey = new TextLongWritable();
                outKey.setText(new Text(session));
                outKey.setCompareValue(new LongWritable(timeTag));

                context.write(outKey,parsedLog);

            } catch (Exception e) {

                LogGenericWritable v = new LogWritable();
                v.put("error_flag", new LogFieldWritable("error"));
                v.put("error_log", new LogFieldWritable(value));
                TextLongWritable outKey = new TextLongWritable();
                int randomKey = (int)(Math.random()*100);
                outKey.setText(new Text("error" + randomKey));
                context.write(outKey, v);

            }
        }
    }

    public static class LogReducer extends Reducer<TextLongWritable, LogGenericWritable, Text, Text> {


//每次用完之后清除用户路径
        private Text sessionId;
        ArrayList<String> users= new ArrayList<String>();
        private JSONArray actionPath = new JSONArray();




        public void setup(Context context) throws IOException, InterruptedException{
            //change .dat file path, to flexable read
            /*
            将ip本地文件拉到节点的代码
            Configuration conf = context.getConfiguration();
            FileSystem fs = FileSystem.get(conf);
            Path ipFile = new Path(conf.get("ip.file.path"));
            Path localPath = new Path(this.getClass().getResource("/").getPath());
            fs.copyToLocalFile(ipFile,localPath);
            */

            IPUtil.load("17monipdb.dat");
        }

        public void reduce(TextLongWritable key, Iterable<LogGenericWritable> values, Context context) throws IOException, InterruptedException{

            Counter validLogCounter = context.getCounter("log Information","Number of valid Logs");
            Counter totalLog = context.getCounter("log Information", "Number of total Logs");
            Counter userCounter = context.getCounter("log Information", "Number of Users");

            Text sid = key.getText();
            if(sessionId == null || !sid.equals(sessionId)){
                sessionId = new Text(sid);
                actionPath.clear();
                //两个session的用户路径不会放在一个path里
            }

            for(LogGenericWritable v : values){

                JSONObject datum = JSON.parseObject(v.asJsonString());
                totalLog.increment(1);

                if(v.getObject("error_flag") == null) {
                    validLogCounter.increment(1);
                    String user = (String) v.getObject("user_id");
                    if(!users.contains(user)){
                        users.add(user);
                        userCounter.increment(1);
                    }

                    String ip = (String) v.getObject("ip");
                    String[] address = IPUtil.find(ip);
                    JSONObject addr = new JSONObject();
                    addr.put("country", address[0]);
                    addr.put("province", address[1]);
                    addr.put("city", address[2]);

                    String activeName = (String) v.getObject("active_name");
                    String reqUrl = (String) v.getObject("req_url");
                    String pathUnit = "pageview".equals(activeName) ? reqUrl : activeName;
                    actionPath.add(pathUnit);
                    datum.put("address", addr);
                    datum.put("action_path", actionPath);
                }
                String outputKey = v.getObject("error_flag") == null? "part" : "error/part";
                context.write(new Text(outputKey), new Text(datum.toJSONString()));
            }
            //在reduce端，ip解析成地址，如果在map端解析，就增加了maptoreduce的量
            //读取文件是不能写在reduce里的，因为reduce是一直while的。所以读取操作要写在reduce外面

        }

    }

    public int run(String[] args) throws Exception {
        Configuration config = getConf();
        config.addResource("mr.xml");
        //config.set("ip.file.path", args[2]);
        //input path, output path, ipfile path
        Job job = Job.getInstance(config);
        job.setJarByClass(ParseLogJob.class);
        job.setJobName("parselog");
        job.setMapperClass(LogMapper.class);
        job.setReducerClass(LogReducer.class);
        job.setMapOutputKeyClass(TextLongWritable.class);
        job.setGroupingComparatorClass(TextLongGroupComparator.class);
        job.setPartitionerClass(TextLongPartitioner.class);
        job.setMapOutputValueClass(LogWritable.class);
        job.setOutputValueClass(Text.class);
        //分布式缓存,把ip文件直接拉到各个节点上
        job.addCacheFile(new URI(config.get("ip.file.path")));
        //将hdfs上面的小文件合并成一个map上的文件进行处理
        job.setInputFormatClass(CombineTextInputFormat.class);
        job.setOutputFormatClass(LogOutputFormat.class);



        FileInputFormat.addInputPath(job, new Path(args[0]));
        Path outputPath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outputPath);

        FileSystem fs = FileSystem.get(config);
        if(fs.exists(outputPath)){
            fs.delete(outputPath,true);
        }

        if (!job.waitForCompletion(true)){
            throw new RuntimeException(job.getJobName()+"failed");
        }
        return 0;
    }

    public static void main(String[] args) throws Exception {
      int res = ToolRunner.run(new Configuration(), new ParseLogJob(), args);
      System.exit(res);

    }
}

