import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

public class DMPerfTest {
    public static void main(String[] args) throws Exception {
        String url = "jdbc:dm://localhost:5236/YOUR_DB"; // 请替换为你的达梦数据库JDBC URL
        String username = "YOUR_USERNAME"; // 替换为你的用户名
        String password = "YOUR_PASSWORD"; // 替换为你的密码

        Class.forName("dm.jdbc.driver.DmDriver"); // 加载达梦JDBC驱动
        
        try (Connection conn = DriverManager.getConnection(url, username, password)) {
            // 写入测试
            String insertSql = "INSERT INTO perf_test (val) VALUES (?)";
            PreparedStatement pstmt = conn.prepareStatement(insertSql);

            int writeRows = 100000;
            long writeStart = System.currentTimeMillis();
            for (int i = 0; i < writeRows; i++) {
                pstmt.setInt(1, i);
                pstmt.addBatch();
                if (i % 1000 == 0 && i > 0) {
                    pstmt.executeBatch();
                }
            }
            pstmt.executeBatch();
            long writeEnd = System.currentTimeMillis();
            double writeSeconds = (writeEnd - writeStart) / 1000.0;
            System.out.println("写入总计行数: " + writeRows + ", 用时: " + writeSeconds + " 秒, 每秒写入: " + (int)(writeRows / writeSeconds) + " 行");

            // 读取测试
            String selectSql = "SELECT * FROM perf_test";
            Statement stmt = conn.createStatement();
            long readStart = System.currentTimeMillis();
            ResultSet rs = stmt.executeQuery(selectSql);

            int readRows = 0;
            while (rs.next()) {
                readRows++;
            }
            long readEnd = System.currentTimeMillis();
            double readSeconds = (readEnd - readStart) / 1000.0;
            System.out.println("读取总计行数: " + readRows + ", 用时: " + readSeconds + " 秒, 每秒读取: " + (int)(readRows / readSeconds) + " 行");
        }
    }
}
// 批量删除 perf_test 表内容，提高效率
// 推荐用 truncate 更高效，但如需 delete 测试批处理：
public static void batchDelete(Connection conn, int batchSize) throws Exception {
    String deleteSql = "DELETE FROM perf_test WHERE val = ?";
    try (PreparedStatement pstmt = conn.prepareStatement(deleteSql)) {
        int count = 0;
        long deleteStart = System.currentTimeMillis();
        // 先查询需要删除的所有 val
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT val FROM perf_test")) {
            while (rs.next()) {
                int val = rs.getInt(1);
                pstmt.setInt(1, val);
                pstmt.addBatch();
                count++;
                if (count % batchSize == 0) {
                    pstmt.executeBatch();
                }
            }
        }
        pstmt.executeBatch(); // 删除剩余
        long deleteEnd = System.currentTimeMillis();
        double deleteSeconds = (deleteEnd - deleteStart) / 1000.0;
        System.out.println("批量删除总计行数: " + count + ", 用时: " + deleteSeconds + " 秒, 每秒删除: " + (int)(count / deleteSeconds) + " 行");
    }
}
