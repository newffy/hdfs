package org.apache.mesos.protobuf;

import org.apache.mesos.Protos;

import java.util.List;

/**
 */
public class CommandInfoUtil {

  public static Protos.CommandInfo.Builder createCommandInfoBuilder() {
    return Protos.CommandInfo.newBuilder();
  }

  public static Protos.CommandInfo createCmdInfo(String cmd,
    List<Protos.CommandInfo.URI> uriList,
    List<Protos.Environment.Variable> executorEnvironment) {
    return createCommandInfoBuilder()
      .addAllUris(uriList)
      .setEnvironment(EnvironmentUtil.createEnvironmentBuilder()
        .addAllVariables(executorEnvironment))
      .setValue(cmd).build();
  }

  public static Protos.CommandInfo.URI createCmdInfoUri(String uri) {
    return Protos.CommandInfo.URI.newBuilder().setValue(uri).build();
  }
}
