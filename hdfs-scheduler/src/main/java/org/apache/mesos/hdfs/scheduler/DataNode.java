package org.apache.mesos.hdfs.scheduler;

import org.apache.mesos.hdfs.config.HdfsFrameworkConfig;
import org.apache.mesos.hdfs.state.IPersistentStateStore;
import org.apache.mesos.hdfs.state.LiveState;
import org.apache.mesos.hdfs.util.HDFSConstants;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.SchedulerDriver;

import java.util.Arrays;
import java.util.List;

/**
 * HDFS Mesos Framework Scheduler class implementation.
 */
public class DataNode extends HdfsNode implements INodeLauncher {
  private String name = HDFSConstants.DATA_NODE_ID;
  private List<String> taskTypes = Arrays.asList(HDFSConstants.DATA_NODE_ID);
  private String executorName = HDFSConstants.NODE_EXECUTOR_ID;

  public DataNode(LiveState liveState, IPersistentStateStore persistentStore, HdfsFrameworkConfig config) {
    super(liveState, persistentStore, config);
  }

  public boolean launch(SchedulerDriver driver, Offer offer) {
    if (offerNotEnoughResources(offer, config.getDataNodeCpus(),
          config.getDataNodeHeapSize())) {
      log.info("Offer does not have enough resources");
      return false;
    }

    boolean launch = false;
    List<String> deadDataNodes = persistenceStore.getDeadDataNodes();
    // TODO (elingg) Relax this constraint to only wait for DN's when the number of DN's is small
    // What number of DN's should we try to recover or should we remove this constraint
    // entirely?
    if (deadDataNodes.isEmpty()) {
      if (persistenceStore.dataNodeRunningOnSlave(offer.getHostname())
        || persistenceStore.nameNodeRunningOnSlave(offer.getHostname())
        || persistenceStore.journalNodeRunningOnSlave(offer.getHostname())) {
        log.info(String.format("Already running hdfs task on %s", offer.getHostname()));
      } else {
        launch = true;
      }
    } else if (deadDataNodes.contains(offer.getHostname())) {
      launch = true;
    }

    if (launch) {
      return launch(
        driver,
        offer,
        name,
        taskTypes,
        executorName);
    }

    return false;
  }
}
