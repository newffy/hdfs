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
public class JournalNode extends HdfsNode implements INodeLauncher {
  private String name = HDFSConstants.JOURNAL_NODE_ID;
  private List<String> taskTypes = Arrays.asList(HDFSConstants.JOURNAL_NODE_ID);
  private String executorName = HDFSConstants.NODE_EXECUTOR_ID;

  public JournalNode(LiveState liveState, IPersistentStateStore persistentStore, HdfsFrameworkConfig config) {
    super(liveState, persistentStore, config);
  }

  public boolean launch(SchedulerDriver driver, Offer offer) {
    if (offerNotEnoughResources(offer, config.getJournalNodeCpus(),
          config.getJournalNodeHeapSize())) {
      log.info("Offer does not have enough resources");
      return false;
    }

    boolean launch = false;
    List<String> deadJournalNodes = persistenceStore.getDeadJournalNodes();

    log.info(deadJournalNodes);

    if (deadJournalNodes.isEmpty()) {
      if (persistenceStore.getJournalNodes().size() == config.getJournalNodeCount()) {
        log.info(String.format("Already running %s journalnodes", config.getJournalNodeCount()));
      } else if (persistenceStore.journalNodeRunningOnSlave(offer.getHostname())) {
        log.info(String.format("Already running journalnode on %s", offer.getHostname()));
      } else if (persistenceStore.dataNodeRunningOnSlave(offer.getHostname())) {
        log.info(String.format("Cannot colocate journalnode and datanode on %s",
              offer.getHostname()));
      } else {
        launch = true;
      }
    } else if (deadJournalNodes.contains(offer.getHostname())) {
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
