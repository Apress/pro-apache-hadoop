package org.apress.prohadoop.c17;

import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Records;

public class ApplicationMaster {    
  public static void main(String[] args) throws Exception {
    final String hdfsPathToPatentFiles = args[0];    
    final String destHdfsFolder = args[1];    
    List<String> files = DownloadUtilities.getFileListing(hdfsPathToPatentFiles);    
    String command = "";    
    // Initialize clients to ResourceManager and NodeManagers
    Configuration conf = new YarnConfiguration();
    AMRMClient<ContainerRequest> rmClient = AMRMClient.createAMRMClient();
    rmClient.init(conf);
    rmClient.start();

    NMClient nmClient = NMClient.createNMClient();
    nmClient.init(conf);
    nmClient.start();

    // Register with ResourceManager
    rmClient.registerApplicationMaster("", 0, "");    
    // Priority for worker containers - priorities are intra-application
    Priority priority = Records.newRecord(Priority.class);
    priority.setPriority(0);

    // Resource requirements for worker containers
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(128);
    capability.setVirtualCores(1);

    // Make container requests to ResourceManager
    for (int i = 0; i < files.size(); ++i) {
      ContainerRequest containerAsk = new ContainerRequest(capability, null, null, priority);
      rmClient.addContainerRequest(containerAsk);
    }

    // Obtain allocated containers and launch 
    int allocatedContainers = 0;
    while (allocatedContainers < files.size()) {
      AllocateResponse response = rmClient.allocate(0);
      for (Container container : response.getAllocatedContainers()) {
        //Command to execute to download url to HDFS
        command = "java org.apress.prohadoop.c17.DownloadFileService" +" " 
                  + files.get(allocatedContainers) + " " +  destHdfsFolder;
        // Launch container by create ContainerLaunchContext
        ContainerLaunchContext ctx = 
            Records.newRecord(ContainerLaunchContext.class);
        ctx.setCommands(Collections.singletonList(command));
        nmClient.startContainer(container, ctx);
        ++allocatedContainers;
      }
      Thread.sleep(100);
    }

    // Now wait for containers to complete
    int completedContainers = 0;
    while (completedContainers < files.size()) {
      AllocateResponse response = rmClient.allocate(completedContainers/files.size());
      for (ContainerStatus status : response.getCompletedContainersStatuses()) {        
        ++completedContainers;
      }
      Thread.sleep(100);
    }

    // Un-register with ResourceManager
    rmClient.unregisterApplicationMaster(
        FinalApplicationStatus.SUCCEEDED, "", "");
  }
}
