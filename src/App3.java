import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import com.github.luben.zstd.ZstdInputStream;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;
public class App3 {
    static Connection connectDB(){
        Connection conn = null;
        try { 
            String url = "jdbc:sqlite:F:/SQLlite/Databases/Destiny/trials_db.db"; //local db connect
            conn = DriverManager.getConnection(url);
        } catch (SQLException e) {System.out.println(e.getMessage());}
        return conn;
    }
    static ZstdInputStream getZstdStream(String filePath) throws FileNotFoundException, IOException{
        File file = new File(filePath);
        InputStream stream = new FileInputStream(file); //actual file: jsonl file compressed with ZStandard, each new JSON object separated by newline, no newline operators in JSON object
        ZstdInputStream stream2 = new ZstdInputStream(stream); //only takes inputstream, overrides the inputstream object
        return stream2;
    }
    public static void main(String[] args) throws Exception{
        Connection conn = connectDB();
        ZstdInputStream stream2 = getZstdStream("E:\\QBit Torrents\\PGCRS\\bungo-pgcr\\9980000000-9990000000.jsonl.zst");
        JsonFactory jFactory = new JsonFactory(); //for actually parsing the JSON
        int redScore = 0;
        int blueScore = 0;
        long id = 0L;
        int mode = 0;
        int r=0;
        int batch = 0;
        String outStr = "";
        long startTime;
        long endTime;
        Statement stmt = conn.createStatement(); //sql statement handling
        try{
            stmt.executeQuery("PRAGMA journal_mode = WAL");
            stmt.execute("PRAGMA synchronous = 0"); //both of these reduce the safety of writing to the DB but they decrease write time significantly
        } catch (SQLException e) {
            System.out.println(e);
        }
        String sql = "INSERT INTO Matches(PGCRID,BlueScore,RedScore) VALUES(?, ?, ?)";
        PreparedStatement pstmt = conn.prepareStatement(sql);
        conn.setAutoCommit(false); //lets me do batch adds for sql
        System.out.println("Started");
        while((r=stream2.read()) != -1){
            startTime = System.currentTimeMillis();
            //outStr = outStr + (char)r;
            System.out.println((char)r);
            endTime = System.currentTimeMillis();
            if(endTime-startTime > 5){
                System.out.println("Stalled!");
                Thread.sleep(10000);
            }
        }
        stream2.close();
        //catches any dangling datapoints
        try{
            System.out.println("Writing batch");
            pstmt.executeBatch();
            conn.commit();
            System.out.println("Batch Executed");
        } catch (SQLException e) {
            System.out.println(e);
        }
    }
}
