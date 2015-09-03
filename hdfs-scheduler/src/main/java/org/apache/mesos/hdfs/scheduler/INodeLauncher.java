package org.apache.mesos.hdfs.scheduler;

import org.apache.mesos.Protos.Offer;
import org.apache.mesos.SchedulerDriver;

import java.util.List;

/**
 * INodeLauncher.
 */
public interface INodeLauncher {
  public boolean launch(SchedulerDriver driver, Offer offer);
}
