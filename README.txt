============================================================
       达梦数据库 JDBC 性能测试工具 - 内网部署指南
============================================================

【运行条件】

1. JDK 环境
   - 需要 JDK 1.8 或更高版本
   - 验证命令: java -version
   - 如果没有JDK，需要从官网下载离线安装包拷贝到内网安装

2. 达梦数据库 JDBC 驱动
   - 文件名通常为: DmJdbcDriver18.jar 或 DmJdbcDriver17.jar
   - 获取位置: 达梦数据库安装目录/drivers/jdbc/
   - 例如: D:\dmdbms\drivers\jdbc\DmJdbcDriver18.jar

3. 达梦数据库服务
   - 确保达梦数据库服务已启动
   - 默认端口: 5236
   - 默认用户: SYSDBA

============================================================

【需要拷贝到内网的文件清单】

□ DmJdbcBenchmark.java    (源代码文件)
□ DmJdbcDriver18.jar      (达梦JDBC驱动，从数据库安装目录获取)
□ run.bat                 (Windows执行脚本)
□ run.sh                  (Linux执行脚本)
□ README.txt              (本说明文件)

============================================================

【修改数据库连接配置】

打开 DmJdbcBenchmark.java，修改第12-16行的配置：

    private static final String URL = "jdbc:dm://localhost:5236/DAMENG";
    private static final String USER = "SYSDBA";
    private static final String PASSWORD = "SYSDBA";

根据实际环境修改：
- localhost  → 数据库服务器IP地址
- 5236       → 数据库端口号
- DAMENG     → 数据库实例名
- SYSDBA     → 数据库用户名
- SYSDBA     → 数据库密码

============================================================

【Windows 执行步骤】

方式一：使用批处理脚本（推荐）
1. 把 DmJdbcDriver18.jar 放到与 DmJdbcBenchmark.java 同一目录
2. 双击运行 run.bat

方式二：手动命令行执行
1. 打开命令提示符(cmd)，进入代码目录
2. 编译: javac -encoding UTF-8 DmJdbcBenchmark.java
3. 运行: java -cp ".;DmJdbcDriver18.jar" DmJdbcBenchmark

============================================================

【Linux 执行步骤】

方式一：使用Shell脚本
1. 把 DmJdbcDriver18.jar 放到与 DmJdbcBenchmark.java 同一目录
2. chmod +x run.sh
3. ./run.sh

方式二：手动命令行执行
1. 进入代码目录
2. 编译: javac -encoding UTF-8 DmJdbcBenchmark.java
3. 运行: java -cp ".:DmJdbcDriver18.jar" DmJdbcBenchmark
   (注意Linux下classpath分隔符是冒号:，不是分号;)

============================================================

【常见问题】

Q1: 提示 "未找到达梦数据库驱动"
A1: 检查 DmJdbcDriver18.jar 是否在当前目录，或检查 -cp 参数路径是否正确

Q2: 提示 "连接被拒绝" 或 "Connection refused"  
A2: 检查数据库服务是否启动，IP和端口是否正确

Q3: 提示 "用户名或密码错误"
A3: 检查 USER 和 PASSWORD 配置是否正确

Q4: 编译时中文乱码
A4: 添加 -encoding UTF-8 参数: javac -encoding UTF-8 DmJdbcBenchmark.java

Q5: 运行时中文乱码
A5: 添加 -Dfile.encoding=UTF-8 参数:
    java -Dfile.encoding=UTF-8 -cp ".;DmJdbcDriver18.jar" DmJdbcBenchmark

============================================================

【测试参数调整】

如需调整测试规模，修改 DmJdbcBenchmark.java 第18-20行：

    private static final int TOTAL_RECORDS = 1000000;  // 总记录数(默认100万)
    private static final int BATCH_SIZE = 10000;       // 批次大小(默认1万)

建议：
- 首次测试可先用 10000 条验证环境是否正常
- 正式测试使用 1000000 条(百万级)

============================================================

