import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


// 新增class Combiner，它是Abstract 需要 extends reducer
// 注意型別
// Mapper跟Combiner output的形態要一致

public class WordCount {
  // Mapper
  // 需要將其中兩個值合併成一個值, 作為 key 或 value
  // 取得"Word", "FileName" 與 "Frequency"
  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{
    private Text keyInfo = new Text(); // Save Word + FileName
    private Text valueInfo = new Text("1"); // Save Frequency, 都指定為1

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());

      while (itr.hasMoreTokens()) {
        String fileName = ((FileSplit) context.getInputSplit()).getPath().getName(); // 取得FileName
        
        keyInfo.set(String.format("%s;%s", itr.nextToken(), fileName));//取得下一個Word跟讀取它的FileName

        context.write(keyInfo, valueInfo);
      }
    }
  }

  // Combiner
  // 將出現的Word跟Frequency次數加總
  // Iterable<Text> => 可迭代Text型態的資料
  public static class Combine extends Reducer<Text,Text,Text,Text>{
    Text valueInfo = new Text();
    Text keyInfo = new Text();
    public void reduce(Text key, Iterable<Text> values, Context context)  throws IOException, InterruptedException{
            int sum = 0; // 計算Frequency
            for(Text value: values){
              sum += Integer.valueOf(value.toString());
            }
            // 切割Word 及 FileName，並加到String Array
            String keyItems[] = key.toString().split(";");

            // 重新設置key為Word
            keyInfo.set(keyItems[0]);
            
            // 重新設置Value為 FileName:Frequency
            valueInfo.set(String.format("%s:%s", keyItems[1],sum));
          
            context.write(keyInfo, valueInfo); // context是將資料寫入給下一層使用 (跟下一層的InputKey, InputValue型態要一致)
      }
  }
        
  // Reducer
  // 將同一個Key它的Value串起來變成字串
  // 生成檔案列表
 public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {
    private Text valueInfo = new Text();
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      String integrateValue = "";
      int a = 0; // 第一個字前綴不要加";"

      for (Text value : values){ // 將Value迭代出來
        if(a == 0){
          integrateValue += value.toString();
        }else{
          integrateValue += ";" + value.toString();
        }
        a = 1;
        
      }
      valueInfo.set(integrateValue);

      context.write(key, valueInfo);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Inverted Index");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(Combine.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}