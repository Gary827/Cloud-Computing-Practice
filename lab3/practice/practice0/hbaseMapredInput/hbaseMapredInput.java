import java.io.IOException;
import java.util.*;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class hbaseMapredInput {
    public static class hbaseMapredInputMapper extends Mapper<LongWritable, Text, Text, MapWritable> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());

            int i = 0;
            MapWritable outputMap = new MapWritable();
            Text outputKey = new Text();

            while (itr.hasMoreTokens()) {
                switch (i) {
                case 0:
                    outputKey.set(itr.nextToken());
                    break;
                case 1:
                    outputMap.put(new Text("family"), new Text(itr.nextToken()));
                    break;
                case 2:
                    outputMap.put(new Text("qualifier"), new Text(itr.nextToken()));
                    break;
                case 3:
                    outputMap.put(new Text("value"), new Text(itr.nextToken()));
                    context.write(outputKey, outputMap);
                    outputMap.clear();
                    break;
                }
                i++;
            }
        }
    }

    public static class hbaseMapredInputReducer extends TableReducer<Text, MapWritable, NullWritable> {
        public void reduce(Text key, Iterable<MapWritable> values, Context context)
                throws IOException, InterruptedException {
            for (MapWritable valueObject : values) {
                String family = valueObject.get(new Text("family")).toString();
                String qualifier = valueObject.get(new Text("qualifier")).toString();
                String value = valueObject.get(new Text("value")).toString();
                Put put = new Put(Bytes.toBytes(key.toString()));
                put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
                context.write(NullWritable.get(), put);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.out.println("please enter input, tablename, username");
            System.exit(0);
        }

        String input = args[0];
        String tablename = args[1];
        String username = args[2]; 
        Configuration config = HBaseConfiguration.create();

        config.set(TableOutputFormat.OUTPUT_TABLE, tablename);
        config.set("hbase.zookeeper.quorum", "master,worker1,worker2");
        config.set("zookeeper.znode.parent", "/hbase-unsecure");

        Job job = Job.getInstance(config);
        job.setJobName("hbaseMapredInput_" + username);

        job.setJarByClass(hbaseMapredInput.class);
        job.setMapperClass(hbaseMapredInputMapper.class);
        job.setReducerClass(hbaseMapredInputReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TableOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(input));
        job.waitForCompletion(true);
    }
}