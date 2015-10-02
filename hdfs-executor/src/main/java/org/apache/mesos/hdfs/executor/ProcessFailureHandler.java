package org.apache.mesos.hdfs.executor;

/**
 * Process failure handler interface.
 */
public interface ProcessFailureHandler {
  public void handle();
}
