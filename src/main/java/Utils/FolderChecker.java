package Utils;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class FolderChecker {

    public static void checkFolder(String outputDir) throws IOException {

        // Ensure the folder exists; otherwise, create the folder.
        if(!Files.exists(Paths.get(outputDir))){
            Files.createDirectories(Paths.get(outputDir));
        }
        else{
            // Clean out the directory
            FileUtils.cleanDirectory(new File(outputDir));
        }
    }
}
