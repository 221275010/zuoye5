<center><div style='height:4mm;'></div><div style="font-family:华文楷体;font-size:25pt;"><b>金融大数据处理技术 作业5（任务一）</b></div></center>

<center><div style='height:2mm;'></div><div style="font-family:华文楷体;font-size:14pt;"><b>221275010 屈航</b></div></center>

# 1、设计思路

## 1.1、 Mapper的设计思路

**`Mapper`重构了一个`map`函数。**

对于`analyst_ratings.csv`里面每一行数据的处理要抽出来**最后一个字段**`stock`。这可以采`string.split(",")`把最后一个字段`stock`提取出来，然后加入`context.write(stock, 1)`。

以下是**`map`函数**的主要功能语句：

```java
tring line = value.toString();
String[] columns = line.split(","); // Assuming CSV format with ',' as delimiter
if (columns.length >= 4) { // Ensure there are enough columns
    stockCode.set(columns[columns.length - 1].trim()); // Assuming stock code is in the last column 
    context.write(stockCode, one);
}
```

*<u>**注意：实验开始前要对`analyst_ratings.csv`中的第一行删去，第一行并不是需要统计的内容。**</u>*

## 1.2、Reducer的设计思路

**`Reducer`包含有`reduce`函数和`cleanup`函数。**

1. **`reduce`函数**就是实现对于同一个key的`Iterable<IntWritable> values`中的每一个元素也就是统计数求和，最后再将求和结果存入一个叫做`Map<String, Integer> stockCountMap = new HashMap<>()`的类里面以待`cleanup`函数实现对输出格式的处理。
   以下是`reduce`函数的主要功能语句：
   
   ```java
   int sum = 0;
   for (IntWritable val : values) {
       sum += val.get();
   }
   stockCountMap.put(key.toString(), sum); 
   ```

2. **`cleanup`函数**就是实现对输出结果的格式控制。首先建立一个列表`List<Map.Entry<String, Integer>> sortedList = new ArrayList<>(stockCountMap.entrySet())`，然后通过**重构`sort`方法**的比较方法实现按照**词频**进行**倒序排列**；最后按照排序好的`sortedList`依次按照格式进行输出。
   以下是**`cleanup`函数**的主要功能语句：

   ```java
   List<Map.Entry<String, Integer>> sortedList = new ArrayList<>(stockCountMap.entrySet());
   sortedList.sort((a, b) -> b.getValue().compareTo(a.getValue())); // Sort by count descending
   
   // Output the results in the required format
   int rank = 1;
   for (Map.Entry<String, Integer> entry : sortedList) {
       context.write(new Text(rank + ": " + entry.getKey() + ", " + entry.getValue()), null);
       rank++;
   }
   ```

## 1.3、项目运行的配置设计

- 此次项目主要使用**`Maven`**进行项目管理，通过编辑**`pom.xml`**文件对该项目进行配置。`pom.xml`文件的配置信息包含有该项目需要哪些库文件需要下载，该项目的项目文件有哪些。


- 依次使用`mvn clean install`进行配置，同时还可以使用`mvn compile`对`.class`文件进行生成，`mvn package`实现对项目文件的`.class`文件打包成`jar`文件。


- 将`analyst_ratings.csv`上传至**HDFS**的`/input`文件夹里面，最后运行该项目的`jar`文件，运行命令为：


```bash
./hadoop jar /home/njucs/zuoye5/target/zuoye5-1.0-SNAPSHOT.jar com.example.StockCount /input /output
```

<u>***注意要把导出来的 part-r-00000解锁，以实现普通用户可以打开，命令如下：***</u>

```bash
sudo chown $USER part-r-00000
```

# 2、程序运行结果

以下即为**`StockCount.java`**程序执行的任务一（统计数据集上市公司股票代码（“stock”列）的出现次数，按出现次数从⼤到⼩输出，输出格式为"<排名>：<股票代码>，<次数>“）的运行结果：

<img src="screenshot\1.png" style="zoom:50%;" />

<img src="screenshot\2.png" style="zoom:50%;" />

<img src="screenshot\3.png" style="zoom:50%;" />

<img src="screenshot\4.png" style="zoom:50%;" />

# 3、WEB页面截图

<img src="screenshot\5.png" style="zoom:50%;" />
