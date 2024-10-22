<center><div style='height:4mm;'></div><div style="font-family:华文楷体;font-size:25pt;"><b>金融大数据处理技术 作业5（任务二）</b></div></center>

<center><div style='height:2mm;'></div><div style="font-family:华文楷体;font-size:14pt;"><b>221275010 屈航</b></div></center>

# 1、设计思路

## 1.1、 Mapper的设计思路

**`Mapper`包含有`setup`函数和`map`函数。**

1. **`Mapper`重构了一个`setup`函数。**该函数的主要作用是将缓存区的停词文件的内容存入`Mapper`类的集合变量`private HashSet<String> stopWords = new HashSet<>()`里面，为了后来的`map`函数对`key`进行去除停词操作。

   获得**缓存文件路径**可以用`context.getLocalCacheFiles()`实现，然后依据该路径建立一个`FileReader`指针**读取缓存文件**的内容，读取的内容暂存在`BufferedReader`当中，最后读取缓存内容将每一个停词**依次存入**`stopWords`当中。

   以下是**`setup`函数**的主要功能语句：

   ```java
   Path[] stopWordPaths = context.getLocalCacheFiles();
   if (stopWordPaths != null && stopWordPaths.length > 0) {
       try (BufferedReader br = new BufferedReader(new FileReader(stopWordPaths[0].toString()))) {
           String line;
           while ((line = br.readLine()) != null) {
               stopWords.add(line.trim().toLowerCase());
           }
       }
   }
   ```

2. **`Mapper`重构了一个`map`函数。**对于`analyst_ratings.csv`里面每一行数据的处理要抽出来第二个个字段“headline”。

   这可以采**正则表达式`String regex =  ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"`**把第二个字段`headline`提取出来；

   对于将**所有字母取小写**和**去掉多余空格**操作可以用`string..trim().toLowerCase()`方法实现；
   
   然后再用一次**正则表达式`"[^a-zA-Z0-9 ]"`**把`headline`里面的**单词提取**出来并放入一个数组`string []`里面；
   
   最后在`context.write(word, one)`之前**检索**一下`word`**是否在停词文件**里面，如果在的话就`continue`，并不写入`context`。
   
   **<u>*注意 1：实验开始前要对`analyst_ratings.csv`中的第一行删去，第一行并不是需要统计的内容。*</u>**
   
   ***<u>注意 2：`split(",")`是不可以将 `analyst_ratings.csv`里面的`headline`字段分出来的，因为`headline`字段里面也有不少`","`。那样会截取出不完整的`headline`，必须要使用正则表达式`String regex =  ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"`才能将`headline`完整提取出来。这一点很重要！</u>***
   
   以下是**`map`函数**的主要功能语句：
   
   ```java
   tring line = value.toString();
   String[] columns = line.split(","); // Assuming CSV format with    ',' as delimiter
   if (columns.length >= 4) { // Ensure there are enough columns
       stockCode.set(columns[columns.length - 1].trim()); // Assuming    stock code is in the last column 
       context.write(stockCode, one);
   }
   ```

## 1.2、Reducer的设计思路

**`Reducer`包含有`reduce`函数和`cleanup`函数。**

1. **`reduce`函数**就是实现对于同一个key的`Iterable<IntWritable> values`中的每一个元素也就是统计数求和，最后再将求和结果存入一个叫做`TreeMap<Integer, String> countMap = new TreeMap<>()`的类里面以待`cleanup`函数实现对输出格式的处理。
   
   **<u>*注意：这里其实将`key`和`value`翻了过来，这样可以更加方便的直接在后面使用关于`value`也就是词频的倒序排序。*</u>**
   
   以下是**`reduce`函数**的主要功能语句：
   
   ```java
   int sum = 0;
   for (IntWritable val : values) {
       sum += val.get();
   }
   countMap.put(sum, key.toString());
   ```
   
2. **`cleanup`函数**就是实现对输出结果的格式控制。因为在`reduce`函数中将**键值颠倒存储**了，所有可以直接调用`Map.Entry<Integer, String> entry : countMap.descendingMap().entrySet()`方法实现关于`key`也就是词频的倒序排列；
   
   该方法返回的是一个**倒序列表**，所以对于该倒序列表只需要`context.write`前100个字段即可，设置一个`for`循环，`rank=1`，最终只输出前100个即可退出循环，输出的每一个字段的`key`是`new Text(rank + ": " + entry.getValue() + ", " + entry.getKey()`，`value`为`null`即可完成格式修改。
   
   以下是**`cleanup`函数**的主要功能语句：
   
   ```java
   int rank = 1;
   for (Map.Entry<Integer, String> entry : countMap.descendingMap().entrySet()) {
       if (rank > 100) break;
       context.write(new Text(rank + ": " + entry.getValue() + ", " + entry.getKey()), null);
       rank++;
   }
   ```

***<u>注意：`main`函数接口中需要添加`job.addCacheFile(new Path(args[2]).toUri());`实现将缓存文件的参数也能传入至该程序的运行。</u>***

## 1.3、项目运行的配置设计

- 此次项目主要使用**`Maven`**进行项目管理，通过编辑**`pom.xml`**文件对该项目进行配置。`pom.xml`文件的配置信息包含有该项目需要哪些库文件需要下载，该项目的项目文件有哪些。


- 依次使用`mvn clean install`进行配置，同时还可以使用`mvn compile`对`.class`文件进行生成，`mvn package`实现对项目文件的`.class`文件打包成`jar`文件。


- 将`analyst_ratings.csv`上传至**HDFS**的`/input`文件夹里面，`stop-word-list.txt`存入HDFS的`/user/root`文件夹里面，最后运行该项目的`jar`文件，运行命令为：


```bash
./hadoop jar /home/njucs/zuoye5_2/target/zuoye5_2-1.0-SNAPSHOT.jar HighFrequencyWords2 /input /output2 /user/root/stop-word-list.txt
```

***<u>注意要把导出来的 part-r-00000解锁，以实现普通用户可以打开，命令如下：</u>***

```bash
sudo chown $USER part-r-00000
```

# 2、程序运行结果

以下即为**`HighFrequencyWords2.java`**程序执行的任务二（统计数据集热点新闻标题（“headline”列）中出现的前100个⾼频单词，按出现次数从⼤到⼩
输出。要求忽略⼤⼩写，忽略标点符号，忽略停词（stop-word-list.txt）。输出格式为"<排名
\>：<单词>，<次数>“）的运行结果：

<img src="screenshot\1.png" style="zoom:50%;" />

<img src="screenshot\2.png" style="zoom:50%;" />

<img src="\screenshot\3.png" style="zoom:50%;" />

<img src="D:\A课程文件夹\大三上课程文件\金融大数据处理技术\金融大数据处理技术作业5（涉及6、7课件）\zuoye5\screenshot\4.png" style="zoom:50%;" />

# 3、WEB页面截图

<img src="screenshot\5.png" style="zoom:50%;" />