package org.apache.mesos.hdfs.executor;

import org.apache.mesos.hdfs.util.HDFSConstants;
import org.apache.mesos.hdfs.util.FailureUtils;

/**
 * When a process fails this handler will throw a runtime exception.
 */
public class ProcessFailureExitHandler implements ProcessFailureHandler {
  public void handle() {
    FailureUtils.exit("Task Process Failed", HDFSConstants.PROC_EXIT_CODE);
  }
}
