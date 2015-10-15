package org.apache.mesos.protobuf;

import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class SlaveUtil {

  private final Logger logger = LoggerFactory.getLogger(getClass());

  public static Protos.SlaveID createSlaveID(String slaveID) {
    return Protos.SlaveID.newBuilder().setValue(slaveID).build();
  }
}
