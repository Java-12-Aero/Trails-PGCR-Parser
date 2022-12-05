import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import com.github.luben.zstd.ZstdInputStream;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
public class App {
    public static void main(String[] args) throws Exception {
        File file = new File("E:\\QBit Torrents\\PGCRS\\bungo-pgcr\\9990000000-10000000000.jsonl.zst");
        InputStream stream = new FileInputStream(file);
        ZstdInputStream stream2 = new ZstdInputStream(stream);
        ObjectMapper mapper = new ObjectMapper();
        JsonFactory jFactory = new JsonFactory();
        String outStr = "";
        int r=0;
        while((char)(r=stream2.read())!='\n'){
            outStr = outStr + (char)r;
        }
        //System.out.println(outStr);
        stream2.close();
        long startTime = System.currentTimeMillis();
        JsonNode json = mapper.readTree(outStr);
        System.out.println(json.get("entries").get(0).get("extended").get("weapons"));
        long endTime = System.currentTimeMillis();
        long time = endTime - startTime;
        System.out.println(time + "ms");
        startTime = System.currentTimeMillis();
        JsonParser jParser = jFactory.createParser(outStr);
        while(jParser.nextToken() != null){
            String fieldName = jParser.getCurrentName();
            System.out.println(fieldName);
            jParser.nextToken();
        }
        endTime = System.currentTimeMillis();
        time = endTime - startTime;
        System.out.println(time+"ms");
    }
}
