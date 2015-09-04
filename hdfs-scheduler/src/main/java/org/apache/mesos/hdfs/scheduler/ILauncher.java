package org.apache.mesos.hdfs.scheduler;

import org.apache.mesos.Protos.Offer;
import org.apache.mesos.SchedulerDriver;

import java.util.List;

/**
 * ILauncher.
 */
public interface ILauncher {
  public void launch(SchedulerDriver driver, Offer offer);
}
