# mylogmnr

1.mylogmnr介绍
此脚本主要是用来整理mysqlbinlog解析binlog得到的文本。只针对binlog用ROW
模式的update，delete，insert语句。整理后的sql文本可以是易读的整个数据库的，也
可以是易读的针对一个表的，同时可以是redo sql或者是undo sql。

<stronger>注意：此脚本可能存在风险，如mysqlbinlog可能会转义某些字符，以及一些未考虑到
的情况。此脚本仅用于测试、诊断问题、学习用途等，不要用于数据恢复等生产环境。使用
此脚本产生的问题本人不承担任何责任。</stronger> 


2.mylogmnr所需条件 
此脚本是用perl编写，这个一般的Linux都有自带。
另外，需要用到DBD::mysql,DBI模块，这个主要用来查询表的元数据。

还需要一个对所有数据库都有只读查询权限的用户(建议操作是使用slave上的)。 

3.mylogmnr使用步骤 
3.1 第一步：使用mysqlbinlog解析对应的binlog,mysqlbinlog最好限制好时间段，这个时间段越少越好
 mysqlbinlog -v --base64-output=DECODE-ROWS /var/lib/mysql/oel58-bin.000006 > 6666666.sql 
 

3.2第二步：mylogmnr.pl使用 

生成整段日志的redo,输出文件为 输入文件名后加“.redo”：
[root@oel58 ~]# perl /home/oracle/mylogmnr.pl -u qrytest -p 123456 -lh 192.168.137.128 -f /root/6666666.sql 

生成整段日志中某个表的redo,输出文件为 输入文件名后加“.redo”：
[root@oel58 ~]# perl /home/oracle/mylogmnr.pl -u qrytest -p 123456 -lh 192.168.137.128 -f /root/6666666.sql -t test.tt

生成整段日志的undo,输出文件为 输入文件名后加“.undo”：
[root@oel58 ~]# perl /home/oracle/mylogmnr.pl -u qrytest -p 123456 -lh 192.168.137.128 -f /root/6666666.sql -o undo

生成整段日志中某个表的undo,输出文件为 输入文件名后加“.undo”：
[root@oel58 ~]# perl /home/oracle/mylogmnr.pl -u qrytest -p 123456 -lh 192.168.137.128 -f /root/6666666.sql -t test.tt -o undo
