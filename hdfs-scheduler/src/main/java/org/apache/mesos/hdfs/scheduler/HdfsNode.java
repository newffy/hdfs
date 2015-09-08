package org.apache.mesos.hdfs.scheduler;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.hdfs.config.HdfsFrameworkConfig;
import org.apache.mesos.hdfs.state.IPersistentStateStore;
import org.apache.mesos.hdfs.state.LiveState;
import org.apache.mesos.hdfs.util.HDFSConstants;
import org.apache.mesos.Protos.CommandInfo;
import org.apache.mesos.Protos.Environment;
import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.Value.Range;
import org.apache.mesos.Protos.Value.Ranges;
import org.apache.mesos.SchedulerDriver;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * HdfsNode base class. 
 */
public abstract class HdfsNode implements IOfferEvaluator, ILauncher {
  private final Log log = LogFactory.getLog(HdfsNode.class);
  private final LiveState liveState;
  private final ResourceFactory resourceFactory;

  protected final HdfsFrameworkConfig config;
  protected final IPersistentStateStore persistenceStore;
  protected final String name;

  public HdfsNode(LiveState liveState, IPersistentStateStore persistentStore, HdfsFrameworkConfig config, String name) {
    this.liveState = liveState;
    this.persistenceStore = persistentStore;
    this.config = config;
    this.name = name;
    this.resourceFactory = new ResourceFactory(config.getHdfsRole());
  }

  public String getName() {
    return name;
  }

  protected abstract String getExecutorName();
  protected abstract List<String> getTaskTypes();

  public void launch(SchedulerDriver driver, Offer offer) {
    List<Task> tasks = createTasks(offer);
    List<TaskInfo> taskInfos = getTaskInfos(tasks);

    recordTasks(tasks);
    driver.launchTasks(Arrays.asList(offer.getId()), taskInfos);
  }

  private List<TaskInfo> getTaskInfos(List<Task> tasks) {
    List<TaskInfo> taskInfos = new ArrayList<TaskInfo>();

    for (Task t : tasks) {
      taskInfos.add(t.getInfo());
    }

    return taskInfos;
  }

  private void recordTasks(List<Task> tasks) {
    for (Task t : tasks) {
      TaskID taskId = t.getId();
      liveState.addStagingTask(taskId);
      persistenceStore.addHdfsNode(taskId, t.getHostname(), t.getType(), t.getName());
    }
  }

  private ExecutorInfo createExecutor(String taskIdName, String nodeName, String executorName) {
    int confServerPort = config.getConfigServerPort();

    String cmd = "export JAVA_HOME=$MESOS_DIRECTORY/" + config.getJreVersion()
      + " && env ; cd hdfs-mesos-* && "
      + "exec `if [ -z \"$JAVA_HOME\" ]; then echo java; "
      + "else echo $JAVA_HOME/bin/java; fi` "
      + "$HADOOP_OPTS "
      + "$EXECUTOR_OPTS "
      + "-cp lib/*.jar org.apache.mesos.hdfs.executor." + executorName;

    return ExecutorInfo
      .newBuilder()
      .setName(nodeName + " executor")
      .setExecutorId(ExecutorID.newBuilder().setValue("executor." + taskIdName).build())
      .addAllResources(getExecutorResources())
      .setCommand(
        CommandInfo
          .newBuilder()
          .addAllUris(
            Arrays.asList(
              CommandInfo.URI
                .newBuilder()
                .setValue(
                  String.format("http://%s:%d/%s", config.getFrameworkHostAddress(),
                    confServerPort,
                    HDFSConstants.HDFS_BINARY_FILE_NAME))
                .build(),
              CommandInfo.URI
                .newBuilder()
                .setValue(
                  String.format("http://%s:%d/%s", config.getFrameworkHostAddress(),
                    confServerPort,
                    HDFSConstants.HDFS_CONFIG_FILE_NAME))
                .build(),
              CommandInfo.URI
                .newBuilder()
                .setValue(config.getJreUrl())
                .build()))
          .setEnvironment(Environment.newBuilder()
            .addAllVariables(Arrays.asList(
              Environment.Variable.newBuilder()
                .setName("LD_LIBRARY_PATH")
                .setValue(config.getLdLibraryPath()).build(),
              Environment.Variable.newBuilder()
                .setName("HADOOP_OPTS")
                .setValue(config.getJvmOpts()).build(),
              Environment.Variable.newBuilder()
                .setName("HADOOP_HEAPSIZE")
                .setValue(String.format("%d", config.getHadoopHeapSize())).build(),
              Environment.Variable.newBuilder()
                .setName("HADOOP_NAMENODE_OPTS")
                .setValue("-Xmx" + config.getNameNodeHeapSize()
                  + "m -Xms" + config.getNameNodeHeapSize() + "m").build(),
              Environment.Variable.newBuilder()
                .setName("HADOOP_DATANODE_OPTS")
                .setValue("-Xmx" + config.getDataNodeHeapSize()
                  + "m -Xms" + config.getDataNodeHeapSize() + "m").build(),
              Environment.Variable.newBuilder()
                .setName("EXECUTOR_OPTS")
                .setValue("-Xmx" + config.getExecutorHeap()
                  + "m -Xms" + config.getExecutorHeap() + "m").build())))
          .setValue(cmd).build())
      .build();
  }

  private List<Resource> createTaskResources(String taskType) {
    double cpu = config.getTaskCpus(taskType);
    double mem = config.getTaskHeapSize(taskType) * config.getJvmOverhead();
    List<Long> ports = config.getTaskPorts(taskType);

    List<Resource> resources = new ArrayList<Resource>();
    resources.add(resourceFactory.createCpuResource(cpu));
    resources.add(resourceFactory.createMemResource(mem));

    for (Long port : ports) {
      resources.add(resourceFactory.createPortResource(port));
    }

    return resources;
  }

  private String getNextTaskName(String taskType) {
    if (taskType.equals(HDFSConstants.NAME_NODE_ID)) {
      Collection<String> nameNodeTaskNames = persistenceStore.getNameNodeTaskNames().values();
      for (int i = 1; i <= HDFSConstants.TOTAL_NAME_NODES; i++) {
        if (!nameNodeTaskNames.contains(HDFSConstants.NAME_NODE_ID + i)) {
          return HDFSConstants.NAME_NODE_ID + i;
        }
      }

      String errorStr = "Cluster is in inconsistent state. " +
        "Trying to launch more namenodes, but they are all already running.";
      log.error(errorStr);
      throw new SchedulerException(errorStr);
    }

    if (taskType.equals(HDFSConstants.JOURNAL_NODE_ID)) {
      Collection<String> journalNodeTaskNames = persistenceStore.getJournalNodeTaskNames().values();
      for (int i = 1; i <= config.getJournalNodeCount(); i++) {
        if (!journalNodeTaskNames.contains(HDFSConstants.JOURNAL_NODE_ID + i)) {
          return HDFSConstants.JOURNAL_NODE_ID + i;
        }
      }

      String errorStr = "Cluster is in inconsistent state. " +
        "Trying to launch more journalnodes, but they all are already running.";
      log.error(errorStr);
      throw new SchedulerException(errorStr);
    }

    return taskType;
  }

  private List<Resource> getExecutorResources() {
    double cpu = config.getExecutorCpus();
    double mem = config.getExecutorHeap() * config.getJvmOverhead();

    return Arrays.asList(
      resourceFactory.createCpuResource(cpu),
      resourceFactory.createMemResource(mem));
  }

  private boolean enoughCpu(Resource resource, double cpu) {
    return cpu + config.getExecutorCpus() < resource.getScalar().getValue();
  }

  private boolean enoughMem(Resource resource, double mem) {
    double taskMem = mem * config.getJvmOverhead();
    double execMem = config.getExecutorHeap() * config.getJvmOverhead();
    return taskMem + execMem < resource.getScalar().getValue();
  }

  private boolean portsAvailable(Resource resource, List<Long> ports) {
    if (resource.getRanges() == null) {
      return false;
    } else {
      for (Long port : ports) {
        if (!portAvailable(resource.getRanges().getRangeList(), port)) {
          return false;
        }
      }
    }

    return true;
  }

  private boolean portAvailable(List<Range> ranges, Long port) {
    for (Range range : ranges) {
      if (port >= range.getBegin() && port <= range.getEnd()) {
        return true;
      }
    }

    return false;
  }

  protected boolean offeredEnoughResources(Offer offer, double cpus, int mem, List<Long> ports) {
    boolean enoughCpu = false;
    boolean enoughMem = false;
    boolean portsAvailable = false;

    for (Resource resource : offer.getResourcesList()) {
      if (resource.getName().equals("cpus") && !enoughCpu) {
        enoughCpu = enoughCpu(resource, cpus);
      }

      if (resource.getName().equals("mem") && !enoughMem) {
        enoughMem = enoughMem(resource, mem);
      }

      if (resource.getName().equals("ports") && !portsAvailable) {
        portsAvailable = portsAvailable(resource, ports);
      }
    }

    return enoughCpu && enoughMem && portsAvailable;
  }

  protected List<Task> createTasks(Offer offer) {
    String executorName = getExecutorName();
    String taskIdName = String.format("%s.%s.%d", name, executorName, System.currentTimeMillis());
    List<Task> tasks = new ArrayList<Task>();

    for (String type : getTaskTypes()) {
      List<Resource> resources = createTaskResources(type);
      ExecutorInfo execInfo = createExecutor(taskIdName, name, executorName);
      String taskName = getNextTaskName(type);

      tasks.add(new Task(resources, execInfo, offer, taskName, type, taskIdName));
    }

    return tasks;
  }
}
