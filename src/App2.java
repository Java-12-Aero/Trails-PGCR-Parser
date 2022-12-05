import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import com.github.luben.zstd.ZstdInputStream;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;
public class App2 {
    public static void main(String[] args) throws Exception {
        Connection conn = null;
        try { 
            String url = "jdbc:sqlite:F:/SQLlite/Databases/Destiny/trials_db.db";
            conn = DriverManager.getConnection(url);
            
        } catch (SQLException e) {System.out.println(e.getMessage());}
        File file = new File("E:\\QBit Torrents\\PGCRS\\bungo-pgcr\\9990000000-10000000000.jsonl.zst");
        InputStream stream = new FileInputStream(file);
        ZstdInputStream stream2 = new ZstdInputStream(stream);
        JsonFactory jFactory = new JsonFactory();
        int redScore = 0;
        int blueScore = 0;
        long id = 0L;
        int r=0;
        int batch = 0;
        String outStr = "";
        Statement stmt = conn.createStatement();
        try{
            stmt.executeQuery("PRAGMA journal_mode = WAL");
            stmt.execute("PRAGMA synchronous = 0");
        } catch (SQLException e) {
            System.out.println(e);
        }
        String sql = "INSERT INTO Matches(PGCRID,BlueScore,RedScore) VALUES(?, ?, ?)";
        PreparedStatement pstmt = conn.prepareStatement(sql);
        conn.setAutoCommit(false);
        while((r=stream2.read())!=-1){
            outStr = outStr + (char)r;
            if((char)r == '\n'){
                JsonParser jParser = jFactory.createParser(outStr);
                while(jParser.nextToken() != null){
                    if("_id".equals(jParser.getCurrentName())){
                        System.out.println("got one!");
                        id = jParser.getValueAsLong();
                        while(jParser.nextToken() != JsonToken.END_OBJECT){
                            if("activityDetails".equals(jParser.getCurrentName())){
                                
                                while(jParser.nextToken() != JsonToken.END_OBJECT){
                                    if("mode".equals(jParser.getCurrentName())){
                                        if(jParser.getValueAsInt() == 84){
                                            
                                            while(jParser.nextToken()!=null){
                                                if("teams".equals(jParser.getCurrentName())){
                                                    while(jParser.nextToken()!=JsonToken.END_ARRAY){
                                                        if("score".equals(jParser.getCurrentName())){
                                                            blueScore = jParser.getValueAsInt();
                                                        }                                                        
                                                    }
                                                    jParser.nextToken();
                                                    while(jParser.nextToken()!=JsonToken.END_ARRAY){
                                                        if("score".equals(jParser.getCurrentName())){
                                                            redScore = jParser.getValueAsInt();
                                                        }                                                        
                                                    }
                                                }
                                            }
                                            pstmt.setLong(1, id);
                                            pstmt.setInt(2,blueScore);
                                            pstmt.setInt(3,redScore);
                                            pstmt.addBatch();
                                            batch++;
                                            if(batch % 1000 == 0){
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
                                    }
                                }
                            }
                        }
                    }
                }
            }
        redScore = 0;
        blueScore = 0;
        outStr = "";
        }
    stream2.close();
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