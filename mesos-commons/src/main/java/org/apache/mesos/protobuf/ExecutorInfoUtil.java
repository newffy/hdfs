package org.apache.mesos.protobuf;

import org.apache.mesos.Protos;

/**
 */
public class ExecutorInfoUtil {

  public static Protos.ExecutorID createExecutorId(String executorId) {
    return Protos.ExecutorID.newBuilder().setValue(executorId).build();
  }

  public static Protos.ExecutorInfo.Builder createExecutorInfoBuilder() {
    return Protos.ExecutorInfo.newBuilder();
  }

}
