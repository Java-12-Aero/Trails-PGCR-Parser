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
public class App3 {
    public static void main(String[] args) throws Exception{
        Connection conn = null;
        try { 
            String url = "jdbc:sqlite:F:/SQLlite/Databases/Destiny/trials_db.db"; //local db connect
            conn = DriverManager.getConnection(url);
        } catch (SQLException e) {System.out.println(e.getMessage());}
        File file = new File("E:\\QBit Torrents\\PGCRS\\bungo-pgcr\\9980000000-9990000000.jsonl.zst"); //actual file: jsonl file compressed with ZStandard, each new JSON object separated by newline, no newline operators in JSON object
        InputStream stream = new FileInputStream(file);
        ZstdInputStream stream2 = new ZstdInputStream(stream); //only takes inputstream, overrides the inputstream object
        JsonFactory jFactory = new JsonFactory(); //for actually parsing the JSON
        int redScore = 0;
        int blueScore = 0;
        long id = 0L;
        int mode = 0;
        int r=0;
        int batch = 0;
        String outStr = "";
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
            //long startTime = System.currentTimeMillis();
            outStr = outStr + (char)r;
            if((char)r == '\n'){
                JsonParser jParser = jFactory.createParser(outStr); //allows the parser to move through the finalized string
                while(jParser.nextToken() != null){
                    if("_id".equals(jParser.getCurrentName())){ 
                        jParser.nextToken();
                        id = jParser.getValueAsLong();
                    }
                    if("mode".equals(jParser.getCurrentName())){
                        jParser.nextToken();
                        if(jParser.getValueAsInt() == 84){
                            mode = 84;
                        } else{
                            mode = -1;
                        }
                    }
                    if("players".equals(jParser.getCurrentName())){
                        jParser.skipChildren(); //skips data I don't care about when parsing
                    }
                    if(mode == 84){
                        if("score".equals(jParser.getCurrentName())){
                            jParser.nextToken();
                            if(blueScore == 0){
                                blueScore = jParser.getValueAsInt();
                            } else {
                                redScore = jParser.getValueAsInt();
                            }
                        }
                    } else if(mode==-1){
                        jParser.skipChildren(); //skips data I don't care about when parsing
                    }
                }
                if(mode == 84){
                    pstmt.setLong(1, id);
                    pstmt.setInt(2,blueScore);
                    pstmt.setInt(3,redScore);
                    pstmt.addBatch(); //creates the statement then adds to batch
                    batch++;
                    //System.out.println(batch);
                    if(batch % 100 == 0){ //executes batch
                        System.out.println(id);
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
                //long endTime = System.currentTimeMillis();
                //System.out.println(endTime-startTime+"ms");
                //cleanup stuff
                blueScore = 0;
                redScore = 0;
                mode = 0;
                outStr = "";
                id = 0L;
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
