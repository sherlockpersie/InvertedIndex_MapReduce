##Inverted Index
###Mapper
实现mapper类，将对每个出现的单词进行映射，key为单词和所处文件名连接而成的字符串，value为该词在该文件中出现次数，初始设定为1。
```
        public static class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {
                // 统计词频时，需要去掉标点符号等符号，此处定义表达式
                private String pattern = "[^a-zA-Z0-9-]";
                @Override
                protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                        // 将每一行转化为一个String
                        String line = value.toString();
                        // 将标点符号等字符用空格替换，这样仅剩单词
                        line = line.replaceAll(pattern, " ");
                        // 将String划分为一个个的单词
                        String[] words = line.split("\\s+");
                        // 将 单词和所处文件名 连接为一个字符串，初始化为词频为1，如果该字符串相同，会传递给combiner做进一步的操作
                        String filename = ((FileSplit)context.getInputSplit()).getPath().getName();
                        for (String word : words) {
                                if (word.length() > 0) {
                                        //单词与文件名之间用冒号标记
                                        context.write(new Text(word + ":" + filename), new Text("1"));
                                }
                        }
                }
        }
```
###Combiner
实现combiner类，实质上也是一种reducer，不过并不是最终的归纳，而是中间过程。在该类的reduce方法中将单词在一个文件中的出现次数进行汇总，并将文件名从key中移到value中与次数连接，使key中只剩下单词。
```
        public static class InvertedIndexCombiner extends Reducer<Text, Text, Text, Text> {
                @Override
                protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
                        // 初始化词频总数为0
                        int cnt = 0;
                        // 对同一文件中出现的相同单词，执行词频汇总操作，也就是将相同key的value累加
                        for (Text value : values) {
                                cnt += Integer.parseInt(value.toString());
                        }
                        //将文件名从key中分离，放到value中与词频连接，使key中只剩单词，
                        //然后就能将同一单词在不同文件中出现的词频连带着文件名以字符串的形式传递给reducer处理
                        int markIndex = key.toString().indexOf(":");
                        context.write(new Text(key.toString().substring(0, markIndex)), 
                                      new Text("(" + key.toString().substring(markIndex + 1) + "," + cnt + ")"));
                }
        }
```
###Reducer
实现reducer类，将同一单词在各文件中的出现情况进行汇总，将不同文件的文件名和该词出现次数连接成一个长字符串，存储在value中。
```
        public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
                @Override
                protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
                        String outValue = new String();
                        // 将同一单词在不同文件中的出现情况汇总
                        outValue += values.iterator().next();
                        for (Text value : values) {
                                outValue += ", " + value.toString();
                        }
                        // 最后输出汇总后的结果，注意输出时，每个单词只会输出一次，紧跟着该单词的词频
                        context.write(key, new Text(outValue));
                }
        }
```
###主函数
```
        public static void main(String[] args) throws Exception {
                // 以下部分为HadoopMapreduce主程序的写法，对照即可
                // 创建配置对象
                Configuration conf = new Configuration();
                // 创建Job对象
                Job job = Job.getInstance(conf, "InvertedIndex");
                // 设置运行Job的类
                job.setJarByClass(InvertedIndex.class);
                // 设置Mapper类
                job.setMapperClass(InvertedIndexMapper.class);
                // 设置Combiner类
                job.setCombinerClass(InvertedIndexCombiner.class);
                // 设置Reducer类
                job.setReducerClass(InvertedIndexReducer.class);
                // 设置Map输出的Key value
                job.setMapOutputKeyClass(Text.class);
                job.setOutputValueClass(IntWritable.class);
                // 设置Reduce输出的Key value
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);
                // 设置输入输出的路径
                FileInputFormat.setInputPaths(job, new Path(args[0]));
                FileOutputFormat.setOutputPath(job, new Path(args[1]));
                // 提交job
                boolean b = job.waitForCompletion(true);
                if(!b) {
                        System.out.println("InvertedIndex task fail!");
                }
        }
```