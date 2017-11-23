# 导出Kafka的数据到文本

一个payload一行, payload内容中的换行符由[br]替换

# Requirement:

 * go
 * git
 * make

# Usage:

`./kfk -c=./kfk.example.yml -d=20171123 -l=1000000 -t=topic_a -o=file_a`

# Todo list

- [ ] 写本地数据库，如sqlite（效率太低，代码暂时没放进来）,leveldb等
- [ ] 行记录过滤
