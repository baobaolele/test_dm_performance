import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.math.BigDecimal;
import java.util.UUID;

public class DmJdbcBenchmark {

    // 数据库连接相关配置，请根据实际环境修改以下参数
    // 驱动类名：达梦数据库官方 JDBC 驱动，需要在项目中引入该 jar 包（如 dm8 jdbc/dm7 jdbc 等）
    private static final String DRIVER_CLASS = "dm.jdbc.driver.DmDriver"; // 达梦 JDBC 驱动类完全限定名

    // 数据库连接URL格式：
    // jdbc:dm://<主机名>:<端口>/<数据库名>
    // 示例：jdbc:dm://localhost:5236/DAMENG
    // - localhost   本地数据库服务器地址，如为远程请改为实际IP或主机名
    // - 5236        达梦数据库默认监听端口，如实际有所修改请同步更改
    // - DAMENG      数据库名称，请使用实际要连接的库名
    private static final String URL = "jdbc:dm://localhost:5236/DAMENG"; // 数据库连接字符串

    // 数据库用户名，如无特殊设置默认可使用SYSDBA（拥有最高权限），实际生产环境请创建专用账号
    private static final String USER = "SYSDBA"; // 数据库用户名

    // 对应数据库账号密码，默认SYSDBA当前密码（如已修改请同步修改），强烈建议勿将生产密码明文写死在代码
    private static final String PASSWORD = "SYSDBA"; // 数据库密码
    /**
     /**
      * 将本地 git 仓库推送到新创建的远程仓库（假设远程还未建立）流程如下：
      *
      * 1. 在 git 代码托管平台（如 GitHub、Gitee、GitLab 等）新建一个空的远程仓库（只需在网页新建，不要初始化任何 readme/license）。
      * 2. 本地已有 git 仓库（若未初始化请先 git init）。
      * 3. 关联远程仓库地址：
      *      git remote add origin <远程仓库地址>
      *      # 例如: git remote add origin https://github.com/yourname/yourrepo.git
      * 4. 推送本地 master/main 分支到远程：
      *      git push -u origin master
      *      # 或新版本 git 默认分支为 main，则用 git push -u origin main
      * 5. 远程仓库代码即同步完成，以后可直接用 git push
      *
      * 注意事项：
      * - 第一次推送用 -u 设定默认上游分支，后续可直接 git push
      * - 远程仓库需为空，否则推送若有冲突需要先拉取合并
      * - 仓库地址建议用 SSH（如 git@github.com:...），或用 HTTPS 并配置访问令牌
      */

    // 测试配置 - 百万级数据
    private static final int TOTAL_RECORDS = 1000000;  // 100万条记录
    private static final int BATCH_SIZE = 10000;       // 每批次1万条

    public static void main(String[] args) {
        Connection conn = null;
        try {
            // 1. 加载驱动
            Class.forName(DRIVER_CLASS);
            System.out.println("========================================");
            System.out.println("达梦数据库 JDBC 性能测试工具");
            System.out.println("========================================");
            System.out.println("驱动加载成功: " + DRIVER_CLASS);

            // 2. 获取连接
            conn = DriverManager.getConnection(URL, USER, PASSWORD);
            conn.setAutoCommit(false);
            System.out.println("数据库连接成功: " + URL);
            System.out.println("测试数据量: " + TOTAL_RECORDS + " 条 (百万级)");
            System.out.println("批次大小: " + BATCH_SIZE + " 条/批");
            System.out.println("========================================\n");

            // 3. 准备测试表（20个字段，多种类型，含主键和索引）
            setupTable(conn);

            // 4. 测试写入效率
            testWritePerformance(conn);

            // 5. 测试读取效率
            testReadPerformance(conn);

            // 6. 测试索引查询效率
            testIndexQueryPerformance(conn);

            // 7. 打印汇总报告
            printSummary();

            // 8. 清理环境
            cleanup(conn);

        } catch (ClassNotFoundException e) {
            System.err.println("未找到达梦数据库驱动，请确保已添加 DmJdbcDriver jar包到classpath中。");
            e.printStackTrace();
        } catch (SQLException e) {
            System.err.println("数据库操作异常: " + e.getMessage());
            e.printStackTrace();
        } finally {
            closeResource(conn);
        }
    }

    // 存储测试结果用于汇总
    private static long writeTimeMs = 0;
    private static long readTimeMs = 0;
    private static long indexQueryTimeMs = 0;
    private static long writeRecordsPerSec = 0;
    private static long readRecordsPerSec = 0;

    /**
     * 创建测试表 - 20个字段，多种数据类型，含主键和索引
     */
    private static void setupTable(Connection conn) throws SQLException {
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            
            // 删除已存在的表
            try {
                stmt.execute("DROP TABLE IF EXISTS TEST_PERFORMANCE_MILLION");
                conn.commit();
            } catch (SQLException e) {
                try {
                    stmt.execute("DROP TABLE TEST_PERFORMANCE_MILLION");
                    conn.commit();
                } catch (SQLException ignored) {
                }
            }

            // 创建测试表 - 20个字段，涵盖字符型、NUMBER整型、NUMBER数值型
            String createSql = "CREATE TABLE TEST_PERFORMANCE_MILLION (" +
                    // ========== NUMBER整型 (无小数位) ==========
                    "ID NUMBER(19) PRIMARY KEY, " +                      // 1. 主键-NUMBER整型
                    "AGE NUMBER(3), " +                                  // 2. 年龄-NUMBER整型(3位)
                    "SCORE NUMBER(5), " +                                // 3. 分数-NUMBER整型(5位)
                    "LEVEL NUMBER(2), " +                                // 4. 等级-NUMBER整型(2位)
                    "IS_ACTIVE NUMBER(1) DEFAULT 1, " +                  // 5. 是否激活-NUMBER整型(1位)
                    "VERSION NUMBER(10) DEFAULT 0, " +                   // 6. 版本号-NUMBER整型(10位)
                    "QUANTITY NUMBER(12), " +                            // 7. 数量-NUMBER整型(12位)
                    // ========== NUMBER数值型 (带小数位) ==========
                    "AMOUNT NUMBER(18,2), " +                            // 8. 金额-NUMBER数值型
                    "RATE NUMBER(10,6), " +                              // 9. 比率-NUMBER数值型(6位小数)
                    "PRICE NUMBER(15,4), " +                             // 10. 单价-NUMBER数值型(4位小数)
                    "WEIGHT NUMBER(12,3), " +                            // 11. 重量-NUMBER数值型(3位小数)
                    "DISCOUNT NUMBER(5,2), " +                           // 12. 折扣-NUMBER数值型(2位小数)
                    // ========== 字符型 ==========
                    "UUID_CODE VARCHAR(50) NOT NULL, " +                 // 13. UUID-变长字符
                    "USER_NAME VARCHAR(100), " +                         // 14. 用户名-变长字符
                    "EMAIL VARCHAR(150), " +                             // 15. 邮箱-变长字符
                    "PHONE CHAR(20), " +                                 // 16. 电话-定长字符
                    "STATUS CHAR(10), " +                                // 17. 状态-定长字符
                    "ADDRESS VARCHAR(500), " +                           // 18. 地址-变长字符
                    "DESCRIPTION CLOB, " +                               // 19. 描述-大字符对象
                    "REMARK VARCHAR(1000)" +                             // 20. 备注-变长字符
                    ")";
            stmt.execute(createSql);
            conn.commit();
            System.out.println("[表结构] TEST_PERFORMANCE_MILLION 创建成功 (20个字段)");

            // 创建索引
            System.out.println("[索引] 正在创建索引...");
            
            // 唯一索引 - 字符型字段
            stmt.execute("CREATE UNIQUE INDEX IDX_UUID ON TEST_PERFORMANCE_MILLION(UUID_CODE)");
            conn.commit();
            System.out.println("  - IDX_UUID (唯一索引-VARCHAR) 创建成功");
            
            // 普通索引 - 字符型字段
            stmt.execute("CREATE INDEX IDX_USER_NAME ON TEST_PERFORMANCE_MILLION(USER_NAME)");
            conn.commit();
            System.out.println("  - IDX_USER_NAME (索引-VARCHAR) 创建成功");
            
            stmt.execute("CREATE INDEX IDX_EMAIL ON TEST_PERFORMANCE_MILLION(EMAIL)");
            conn.commit();
            System.out.println("  - IDX_EMAIL (索引-VARCHAR) 创建成功");
            
            // 普通索引 - NUMBER整型字段
            stmt.execute("CREATE INDEX IDX_SCORE ON TEST_PERFORMANCE_MILLION(SCORE)");
            conn.commit();
            System.out.println("  - IDX_SCORE (索引-NUMBER整型) 创建成功");
            
            // 组合索引 - 字符型 + NUMBER整型
            stmt.execute("CREATE INDEX IDX_STATUS_LEVEL ON TEST_PERFORMANCE_MILLION(STATUS, LEVEL)");
            conn.commit();
            System.out.println("  - IDX_STATUS_LEVEL (组合索引-CHAR+NUMBER) 创建成功");

            System.out.println("[准备完成] 表和索引创建完毕\n");

        } finally {
            if (stmt != null) stmt.close();
        }
    }

    /**
     * 测试写入性能 - 百万级数据批量插入
     */
    private static void testWritePerformance(Connection conn) throws SQLException {
        System.out.println(">>> 开始写入测试 <<<");
        System.out.println("目标: " + TOTAL_RECORDS + " 条记录");
        System.out.println("批次: 每 " + BATCH_SIZE + " 条提交一次");
        System.out.println("");

        // 按字段顺序: NUMBER整型(7个) -> NUMBER数值型(5个) -> 字符型(8个)
        String sql = "INSERT INTO TEST_PERFORMANCE_MILLION (" +
                "ID, AGE, SCORE, LEVEL, IS_ACTIVE, VERSION, QUANTITY, " +
                "AMOUNT, RATE, PRICE, WEIGHT, DISCOUNT, " +
                "UUID_CODE, USER_NAME, EMAIL, PHONE, STATUS, ADDRESS, DESCRIPTION, REMARK" +
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        PreparedStatement pstmt = null;
        long startTime = System.currentTimeMillis();
        int processedCount = 0;

        String[] statuses = {"ACTIVE", "INACTIVE", "PENDING", "DELETED", "ARCHIVED"};

        try {
            pstmt = conn.prepareStatement(sql);

            for (int i = 0; i < TOTAL_RECORDS; i++) {
                int idx = 1;
                
                // ========== NUMBER整型 (7个字段) ==========
                // 1. ID - 主键 NUMBER(19)
                pstmt.setLong(idx++, i + 1);
                // 2. AGE - 年龄 NUMBER(3)
                pstmt.setInt(idx++, 18 + (i % 60));
                // 3. SCORE - 分数 NUMBER(5)
                pstmt.setInt(idx++, i % 10000);
                // 4. LEVEL - 等级 NUMBER(2)
                pstmt.setInt(idx++, i % 100);
                // 5. IS_ACTIVE - 是否激活 NUMBER(1)
                pstmt.setInt(idx++, i % 2);
                // 6. VERSION - 版本号 NUMBER(10)
                pstmt.setLong(idx++, i % 1000000);
                // 7. QUANTITY - 数量 NUMBER(12)
                pstmt.setLong(idx++, (i % 100000) * 100L);
                
                // ========== NUMBER数值型 (5个字段) ==========
                // 8. AMOUNT - 金额 NUMBER(18,2)
                pstmt.setBigDecimal(idx++, new BigDecimal(String.format("%.2f", (i % 100000) * 12.34)));
                // 9. RATE - 比率 NUMBER(10,6)
                pstmt.setBigDecimal(idx++, new BigDecimal(String.format("%.6f", (i % 1000) / 1000.0)));
                // 10. PRICE - 单价 NUMBER(15,4)
                pstmt.setBigDecimal(idx++, new BigDecimal(String.format("%.4f", (i % 10000) * 0.9999)));
                // 11. WEIGHT - 重量 NUMBER(12,3)
                pstmt.setBigDecimal(idx++, new BigDecimal(String.format("%.3f", (i % 10000) * 1.234)));
                // 12. DISCOUNT - 折扣 NUMBER(5,2)
                pstmt.setBigDecimal(idx++, new BigDecimal(String.format("%.2f", (i % 100) / 100.0)));
                
                // ========== 字符型 (8个字段) ==========
                // 13. UUID_CODE - VARCHAR(50)
                pstmt.setString(idx++, UUID.randomUUID().toString());
                // 14. USER_NAME - VARCHAR(100)
                pstmt.setString(idx++, "User_" + String.format("%07d", i));
                // 15. EMAIL - VARCHAR(150)
                pstmt.setString(idx++, "user" + i + "@example.com");
                // 16. PHONE - CHAR(20)
                pstmt.setString(idx++, "138" + String.format("%08d", i % 100000000));
                // 17. STATUS - CHAR(10)
                pstmt.setString(idx++, statuses[i % statuses.length]);
                // 18. ADDRESS - VARCHAR(500)
                pstmt.setString(idx++, "中国北京市朝阳区某某街道" + (i % 1000) + "号楼" + (i % 100) + "单元");
                // 19. DESCRIPTION - CLOB
                pstmt.setString(idx++, "这是第" + i + "条测试数据的详细描述信息，用于测试CLOB大字符对象的读写性能，包含中英文混合内容Test Data。");
                // 20. REMARK - VARCHAR(1000)
                pstmt.setString(idx++, "备注信息_" + i + "_" + UUID.randomUUID().toString().substring(0, 8));

                pstmt.addBatch();

                if ((i + 1) % BATCH_SIZE == 0) {
                    pstmt.executeBatch();
                    conn.commit();
                    processedCount = i + 1;
                    
                    // 打印进度
                    long elapsed = System.currentTimeMillis() - startTime;
                    double progress = (processedCount * 100.0) / TOTAL_RECORDS;
                    long currentTps = (elapsed > 0) ? (processedCount * 1000L / elapsed) : 0;
                    System.out.printf("  进度: %6d / %d (%.1f%%) | 当前TPS: %,d 条/秒%n", 
                            processedCount, TOTAL_RECORDS, progress, currentTps);
                }
            }

            // 处理剩余数据（如果有）
            pstmt.executeBatch();
            conn.commit();

        } catch (SQLException e) {
            conn.rollback();
            throw e;
        } finally {
            if (pstmt != null) pstmt.close();
        }

        long endTime = System.currentTimeMillis();
        writeTimeMs = endTime - startTime;
        double seconds = writeTimeMs / 1000.0;
        writeRecordsPerSec = (seconds > 0) ? (long) (TOTAL_RECORDS / seconds) : TOTAL_RECORDS;

        System.out.println("");
        System.out.println("==================================================");
        System.out.println("【写入测试完成】");
        System.out.println("--------------------------------------------------");
        System.out.printf("  总记录数:     %,d 条%n", TOTAL_RECORDS);
        System.out.printf("  整体写入时间: %,d ms (%.2f 秒)%n", writeTimeMs, seconds);
        System.out.printf("  每秒写入数据: %,d 条/秒 (TPS)%n", writeRecordsPerSec);
        System.out.println("==================================================\n");
    }

    /**
     * 测试读取性能 - 全表扫描
     */
    private static void testReadPerformance(Connection conn) throws SQLException {
        System.out.println(">>> 开始读取测试 (全表扫描) <<<\n");

        String sql = "SELECT * FROM TEST_PERFORMANCE_MILLION";
        Statement stmt = null;
        ResultSet rs = null;

        long startTime = System.currentTimeMillis();
        int count = 0;

        try {
            stmt = conn.createStatement();
            stmt.setFetchSize(BATCH_SIZE);
            rs = stmt.executeQuery(sql);

            while (rs.next()) {
                // ========== 读取NUMBER整型 (7个字段) ==========
                rs.getLong("ID");
                rs.getInt("AGE");
                rs.getInt("SCORE");
                rs.getInt("LEVEL");
                rs.getInt("IS_ACTIVE");
                rs.getLong("VERSION");
                rs.getLong("QUANTITY");
                
                // ========== 读取NUMBER数值型 (5个字段) ==========
                rs.getBigDecimal("AMOUNT");
                rs.getBigDecimal("RATE");
                rs.getBigDecimal("PRICE");
                rs.getBigDecimal("WEIGHT");
                rs.getBigDecimal("DISCOUNT");
                
                // ========== 读取字符型 (8个字段) ==========
                rs.getString("UUID_CODE");
                rs.getString("USER_NAME");
                rs.getString("EMAIL");
                rs.getString("PHONE");
                rs.getString("STATUS");
                rs.getString("ADDRESS");
                rs.getString("DESCRIPTION");  // CLOB
                rs.getString("REMARK");
                
                count++;

                // 每50万条打印一次进度
                if (count % 500000 == 0) {
                    long elapsed = System.currentTimeMillis() - startTime;
                    System.out.printf("  已读取: %,d 条 | 耗时: %.2f 秒%n", count, elapsed / 1000.0);
                }
            }
        } finally {
            if (rs != null) rs.close();
            if (stmt != null) stmt.close();
        }

        long endTime = System.currentTimeMillis();
        readTimeMs = endTime - startTime;
        double seconds = readTimeMs / 1000.0;
        readRecordsPerSec = (seconds > 0) ? (long) (count / seconds) : count;

        System.out.println("");
        System.out.println("==================================================");
        System.out.println("【读取测试完成】");
        System.out.println("--------------------------------------------------");
        System.out.printf("  成功读取:     %,d 条%n", count);
        System.out.printf("  整体读取时间: %,d ms (%.2f 秒)%n", readTimeMs, seconds);
        System.out.printf("  每秒读取数据: %,d 条/秒%n", readRecordsPerSec);
        System.out.println("==================================================\n");
    }

    /**
     * 测试索引查询性能
     */
    private static void testIndexQueryPerformance(Connection conn) throws SQLException {
        System.out.println(">>> 开始索引查询测试 <<<\n");

        Statement stmt = null;
        ResultSet rs = null;
        long startTime = System.currentTimeMillis();
        int totalQueries = 0;

        try {
            stmt = conn.createStatement();

            // 测试1: 主键查询
            System.out.println("[测试1] 主键精确查询 (1000次)...");
            long t1 = System.currentTimeMillis();
            for (int i = 0; i < 1000; i++) {
                rs = stmt.executeQuery("SELECT * FROM TEST_PERFORMANCE_MILLION WHERE ID = " + (i * 1000 + 1));
                while (rs.next()) { rs.getLong("ID"); }
                rs.close();
                totalQueries++;
            }
            System.out.printf("  完成: %d ms%n", System.currentTimeMillis() - t1);

            // 测试2: 唯一索引查询
            System.out.println("[测试2] 用户名索引查询 (1000次)...");
            long t2 = System.currentTimeMillis();
            for (int i = 0; i < 1000; i++) {
                rs = stmt.executeQuery("SELECT * FROM TEST_PERFORMANCE_MILLION WHERE USER_NAME = 'User_" + 
                        String.format("%07d", i * 1000) + "'");
                while (rs.next()) { rs.getString("USER_NAME"); }
                rs.close();
                totalQueries++;
            }
            System.out.printf("  完成: %d ms%n", System.currentTimeMillis() - t2);

            // 测试3: 范围查询
            System.out.println("[测试3] 范围查询 (100次，每次约1万条)...");
            long t3 = System.currentTimeMillis();
            for (int i = 0; i < 100; i++) {
                int start = i * 10000;
                rs = stmt.executeQuery("SELECT COUNT(*) FROM TEST_PERFORMANCE_MILLION WHERE ID BETWEEN " + 
                        start + " AND " + (start + 10000));
                while (rs.next()) { rs.getInt(1); }
                rs.close();
                totalQueries++;
            }
            System.out.printf("  完成: %d ms%n", System.currentTimeMillis() - t3);

            // 测试4: 组合索引查询 (STATUS + LEVEL)
            System.out.println("[测试4] 组合索引查询 (500次)...");
            String[] statuses = {"ACTIVE", "INACTIVE", "PENDING", "DELETED", "ARCHIVED"};
            long t4 = System.currentTimeMillis();
            for (int i = 0; i < 500; i++) {
                rs = stmt.executeQuery("SELECT COUNT(*) FROM TEST_PERFORMANCE_MILLION WHERE STATUS = '" + 
                        statuses[i % 5] + "' AND LEVEL = " + (i % 100));
                while (rs.next()) { rs.getInt(1); }
                rs.close();
                totalQueries++;
            }
            System.out.printf("  完成: %d ms%n", System.currentTimeMillis() - t4);
            
            // 测试5: NUMBER数值型字段范围查询
            System.out.println("[测试5] NUMBER数值型范围查询 (200次)...");
            long t5 = System.currentTimeMillis();
            for (int i = 0; i < 200; i++) {
                BigDecimal minAmount = new BigDecimal(i * 5000);
                BigDecimal maxAmount = new BigDecimal((i + 1) * 5000);
                rs = stmt.executeQuery("SELECT COUNT(*) FROM TEST_PERFORMANCE_MILLION WHERE AMOUNT BETWEEN " + 
                        minAmount + " AND " + maxAmount);
                while (rs.next()) { rs.getInt(1); }
                rs.close();
                totalQueries++;
            }
            System.out.printf("  完成: %d ms%n", System.currentTimeMillis() - t5);

        } finally {
            if (stmt != null) stmt.close();
        }

        long endTime = System.currentTimeMillis();
        indexQueryTimeMs = endTime - startTime;
        double avgQueryTime = (double) indexQueryTimeMs / totalQueries;

        System.out.println("");
        System.out.println("==================================================");
        System.out.println("【索引查询测试完成】");
        System.out.println("--------------------------------------------------");
        System.out.printf("  总查询次数:   %,d 次%n", totalQueries);
        System.out.printf("  整体耗时:     %,d ms (%.2f 秒)%n", indexQueryTimeMs, indexQueryTimeMs / 1000.0);
        System.out.printf("  平均查询时间: %.2f ms/次%n", avgQueryTime);
        System.out.printf("  查询QPS:      %,d 次/秒%n", (indexQueryTimeMs > 0) ? (totalQueries * 1000L / indexQueryTimeMs) : totalQueries);
        System.out.println("==================================================\n");
    }

    /**
     * 打印汇总报告
     */
    private static void printSummary() {
        System.out.println("");
        System.out.println("########################################################");
        System.out.println("#                    性 能 测 试 汇 总                   #");
        System.out.println("########################################################");
        System.out.println("");
        System.out.printf("  数据规模:        %,d 条 (百万级)%n", TOTAL_RECORDS);
        System.out.println("  表字段数:        20 个");
        System.out.println("    - NUMBER整型:  7 个 (ID, AGE, SCORE, LEVEL, IS_ACTIVE, VERSION, QUANTITY)");
        System.out.println("    - NUMBER数值型: 5 个 (AMOUNT, RATE, PRICE, WEIGHT, DISCOUNT)");
        System.out.println("    - 字符型:      8 个 (UUID_CODE, USER_NAME, EMAIL, PHONE, STATUS, ADDRESS, DESCRIPTION, REMARK)");
        System.out.println("  索引数量:        5 个 (含唯一索引、组合索引)");
        System.out.println("");
        System.out.println("  ┌─────────────────┬────────────────┬────────────────┐");
        System.out.println("  │     测试项      │    总耗时      │    吞吐量      │");
        System.out.println("  ├─────────────────┼────────────────┼────────────────┤");
        System.out.printf("  │ 批量写入        │ %,8d ms    │ %,8d 条/秒 │%n", writeTimeMs, writeRecordsPerSec);
        System.out.printf("  │ 全表读取        │ %,8d ms    │ %,8d 条/秒 │%n", readTimeMs, readRecordsPerSec);
        System.out.printf("  │ 索引查询        │ %,8d ms    │     -          │%n", indexQueryTimeMs);
        System.out.println("  └─────────────────┴────────────────┴────────────────┘");
        System.out.println("");
        System.out.println("########################################################\n");
    }

    /**
     * 清理测试环境
     */
    private static void cleanup(Connection conn) throws SQLException {
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            stmt.execute("DROP TABLE TEST_PERFORMANCE_MILLION");
            conn.commit();
            System.out.println("[清理完成] 测试表 TEST_PERFORMANCE_MILLION 已删除");
        } finally {
            if (stmt != null) stmt.close();
        }
    }

    private static void closeResource(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
                System.out.println("[连接关闭] 数据库连接已释放");
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
