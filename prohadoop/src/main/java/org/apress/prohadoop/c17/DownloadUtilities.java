package org.apress.prohadoop.c17;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class DownloadUtilities {
    
    public static boolean doesFileExists(FileSystem fs,Path outFile) throws Exception {
        
        return fs.exists(outFile);
    }
    
    public static String getFilePathFromURL(String rootPath, String fileUrl){
        String filePath =  rootPath + "/" + fileUrl.substring(fileUrl.lastIndexOf("/")+1);
        return filePath;
    }
    public static List<String> getFileListing(String patentListFile) throws Exception{
        List<String> lines = new ArrayList<String>();
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path p = new Path(patentListFile);
        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(p)));
        String line;
        line=br.readLine();
        System.out.println(line);
        if(!StringUtils.isBlank(line)){
            lines.add(line);
        }
        while (line != null){                
                line=br.readLine();
                if(!StringUtils.isBlank(line)){
                    lines.add(line);
                }
        }
        return lines;
        
    }


}
