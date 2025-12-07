#!/bin/bash
echo "============================================"
echo "  达梦数据库 JDBC 性能测试工具"
echo "============================================"
echo ""

# 设置JDBC驱动路径（可修改为实际路径）
JDBC_DRIVER="DmJdbcDriver18.jar"

# 检查JDBC驱动是否存在
if [ ! -f "$JDBC_DRIVER" ]; then
    echo "[错误] 未找到达梦JDBC驱动: $JDBC_DRIVER"
    echo ""
    echo "请确保以下文件之一存在于当前目录:"
    echo "  - DmJdbcDriver18.jar"
    echo "  - DmJdbcDriver17.jar"
    echo ""
    echo "驱动文件通常位于达梦安装目录:"
    echo "  /opt/dmdbms/drivers/jdbc/DmJdbcDriver18.jar"
    echo ""
    exit 1
fi

# 检查Java环境
if ! command -v java &> /dev/null; then
    echo "[错误] 未检测到Java环境，请先安装JDK"
    exit 1
fi

echo "[1/2] 正在编译 DmJdbcBenchmark.java ..."
javac -encoding UTF-8 DmJdbcBenchmark.java
if [ $? -ne 0 ]; then
    echo "[错误] 编译失败"
    exit 1
fi
echo "     编译成功!"
echo ""

echo "[2/2] 正在执行性能测试..."
echo ""
java -Dfile.encoding=UTF-8 -cp ".:$JDBC_DRIVER" DmJdbcBenchmark

echo ""
echo "============================================"
echo "  测试完成"
echo "============================================"

