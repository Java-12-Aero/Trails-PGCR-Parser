import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import com.github.luben.zstd.ZstdInputStream;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;

class readFile implements Runnable{
    private Thread t;
    private String fileName;
    private String path;
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
    readFile(String name){
        fileName = name;
        path = String.format("E:\\QBit Torrents\\PGCRS\\bungo-pgcr\\%s.jsonl.zst",fileName);
    }
    public void run(){
        try{
            Connection conn = connectDB();
            ZstdInputStream stream = getZstdStream(path);
            JsonFactory jFactory = new JsonFactory(); //for actually parsing the JSON
            JsonParser jParser = jFactory.createParser(stream);
            Long id = 0L;
            int mode = 0;
            int blueScore = 0;
            int redScore = 0;
            int batch = 0;
            String sql = "INSERT INTO Matches(PGCRID,BlueScore,RedScore) VALUES(?, ?, ?)";
            PreparedStatement pstmt = conn.prepareStatement(sql);
            conn.setAutoCommit(false); //lets me do batch adds for sql
            System.out.println("Started");
            while(jParser.nextValue() != null){
                if("_id".equals(jParser.getCurrentName())){
                    if(mode == 84){
                        batch++;
                        pstmt.setLong(1,id);
                        pstmt.setInt(2,blueScore);
                        pstmt.setInt(3,redScore);
                        pstmt.addBatch();
                        if(batch % 10000 == 0){
                            writeDB writer = new writeDB(pstmt);
                            writer.start();
                        }
                    }
                    id = jParser.getValueAsLong();
                    mode = 0;
                    blueScore = 0;
                    redScore = 0;
                }
                if("entries".equals(jParser.getCurrentName())){
                    jParser.skipChildren();
                }
                if("mode".equals(jParser.getCurrentName())){
                    mode = jParser.getIntValue();
                }
                if(mode == 84 && "score".equals(jParser.getCurrentName())){
                    if(blueScore == 0){
                        blueScore = jParser.getIntValue();
                    } else {
                        redScore = jParser.getIntValue();
                    }
                }
            }
            stream.close();
            System.out.println(id);
            writeDB writer = new writeDB(pstmt);
            writer.start();
        } catch (Exception e) {
            System.out.println(e);
        }
    }
    public void start(){
        if(t==null){
            t = new Thread(this,fileName);
            t.start();
        }
    }
}
class writeDB implements Runnable {
    private Thread t;
    private PreparedStatement pstmt;
    private Connection conn = connectDB();
    writeDB(PreparedStatement statement){
        pstmt = statement;
    }
    static Connection connectDB(){
        Connection conn = null;
        try { 
            String url = "jdbc:sqlite:F:/SQLlite/Databases/Destiny/trials_db.db"; //local db connect
            conn = DriverManager.getConnection(url);
        } catch (SQLException e) {System.out.println(e.getMessage());}
        return conn;
    }
    public void run(){
        Statement stmt;
        try {
            stmt = conn.createStatement();
            try{
                stmt.executeQuery("PRAGMA journal_mode = WAL");
                stmt.execute("PRAGMA synchronous = 0"); //both of these reduce the safety of writing to the DB but they decrease write time significantly
            } catch (SQLException e) {
                System.out.println(e);
            }
        } catch (SQLException e1) {
            e1.printStackTrace();
        }
        
        System.out.println("writing");
        try {
            pstmt.executeBatch();
            conn.commit();
        } catch (SQLException e) {
            System.out.println(e);
        }
    }
    public void start(){
        if(t==null){
            t = new Thread(this, "batchWriter");
            t.start();
        }
    }
}

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
        for(long i = 5400000000L; i<10000000000L; i+= 10000000L){
            String name = String.format("%d-%d",i,i+10000000L);
            readFile reader = new readFile(name);
            reader.start();
        }
        
    }
}
