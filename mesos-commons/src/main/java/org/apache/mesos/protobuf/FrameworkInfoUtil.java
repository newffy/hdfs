package org.apache.mesos.protobuf;

import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class FrameworkInfoUtil {

  private final Logger logger = LoggerFactory.getLogger(getClass());

  public static Protos.FrameworkID createFrameworkId(String name) {
    return Protos.FrameworkID.newBuilder().setValue(name).build();
  }

}
