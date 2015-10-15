package org.apache.mesos.protobuf;

import org.apache.mesos.Protos;

/**
 */
public class EnvironmentUtil {

  public static Protos.Environment.Variable createEnvironment(String key, String value) {
    return Protos.Environment.Variable.newBuilder().setName(key).setValue(value).build();
  }

  public static Protos.Environment.Builder createEnvironmentBuilder() {
    return Protos.Environment.newBuilder();
  }

}

