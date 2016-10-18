package org.apress.prohadoop.c11;
import java.util.HashMap;
import java.util.Map;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
public class EmbeddedPigServer {

    public static void main(String[] args) throws Exception {
        boolean isLocal = Boolean.parseBoolean(args[0]);
        String pigScriptPath = args[1];
        String parameters = args[2];
        String[] params = parameters.split(",");
        Map<String, String> paramMap = new HashMap<String, String>();
        for (String p : params) {
            String[] param = p.split("=");
            paramMap.put(param[0], param[1]);
        }

        PigServer pigServer;
        if (isLocal) {
            pigServer = new PigServer(ExecType.LOCAL);
        } else {
            pigServer = new PigServer(ExecType.MAPREDUCE);
        }
        pigServer.registerScript(pigScriptPath, paramMap);
    }

}
// hadoop EmbeddedPigServer true scripts/pig/sampleparameterized.pig
// A_LOC=input/pigsample/a.txt,B_LOC=input/pigsample/b.txt,OUTPUT_LOC=sampleout
