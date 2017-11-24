# 导出Kafka的数据到文本

一个payload一行, payload内容中的换行符由[br]替换

# Requirement:

 * go
 * git
 * make

# Usage:

`./kfk -c=./kfk.example.yml -d=20171123 -l=1000000 -t=topic_a -o=file_a`

```bash
Usage of ./kfk:
  -bs int
        batch write size (default 10000)
  -c string
        set config file (default "./kfk.yml")
  -d int
        set one day, higher priorty than start/end
  -e int
        set end date
  -f string
        output format, available: text, csv (default "text")
  -fields string
        output fields limit, payload must be json string format
  -l int
        split files when reach lines,0=no split
  -o string
        output file, '-'=stdout (default "-")
  -s int
        set start date
  -t string
        set topic
```

# Todo list

- [ ] 写本地数据库，如sqlite（效率太低，代码暂时没放进来）,leveldb等
- [ ] 行记录过滤
