package org.apache.mesos.hdfs.scheduler;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.Protos.CommandInfo;
import org.apache.mesos.Protos.Environment;
import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.Filters;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.hdfs.config.HdfsFrameworkConfig;
import org.apache.mesos.hdfs.state.HdfsState;
import org.apache.mesos.hdfs.util.HDFSConstants;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.List;

/**
 * HdfsNode base class.
 */
public abstract class HdfsNode implements IOfferEvaluator, ILauncher {
  private final Log log = LogFactory.getLog(HdfsNode.class);
  private final ResourceFactory resourceFactory;

  protected final HdfsFrameworkConfig config;
  protected final HdfsState state;
  protected final String name;

  protected abstract String getExecutorName();
  protected abstract List<String> getTaskTypes();

  public HdfsNode(
      HdfsState state,
      HdfsFrameworkConfig config,
      String name) {
    this.state = state;
    this.config = config;
    this.name = name;
    this.resourceFactory = new ResourceFactory(config);
  }

  public String getName() {
    return name;
  }

  public void launch(SchedulerDriver driver, Offer offer)
    throws ClassNotFoundException, IOException, InterruptedException, ExecutionException {
    List<Task> tasks = createTasks(offer);

    // The recording of Tasks is what can potentially throw the exceptions noted above.  This is good news
    // because we are guaranteed that we do not actually launch Tasks unless we have recorded them.
    recordTasks(tasks);

    List<OfferID> offerIds = getOfferIds(offer);
    List<Offer.Operation> operations = getLaunchOperations(tasks);
    Filters filters = getFilters();

    driver.acceptOffers(offerIds, operations, filters);
  }

  private void reserveOffer(Scheduler driver, Offer offer) {

  }

  private List<OfferID> getOfferIds(Offer offer) {
    List<OfferID> offerIds = new ArrayList<OfferID>();
    offerIds.add(offer.getId());
    return offerIds;
  }

  private List<Offer.Operation> getLaunchOperations(List<Task> tasks) {
    Offer.Operation.Launch.Builder launch = getLaunchBuilder(tasks);
    List<Offer.Operation> operations = new ArrayList<Offer.Operation>();

    Offer.Operation operation = Offer.Operation.newBuilder()
      .setType(Offer.Operation.Type.LAUNCH)
      .setLaunch(launch)
      .build();

    operations.add(operation);
    return operations;
  }

  private Offer.Operation createReserveOperation() {
  }

  private Offer.Operation.Launch.Builder getLaunchBuilder(List<Task> tasks) {
    List<TaskInfo> taskInfos = getTaskInfos(tasks);

    Offer.Operation.Launch.Builder launch = Offer.Operation.Launch.newBuilder();
    for (TaskInfo info : taskInfos) {
      launch.addTaskInfos(TaskInfo.newBuilder(info));
    }

    return launch;
  }

  private Filters getFilters() {
    return Filters.newBuilder().setRefuseSeconds(1).build();
  }

  private List<TaskInfo> getTaskInfos(List<Task> tasks) {
    List<TaskInfo> taskInfos = new ArrayList<TaskInfo>();

    for (Task task : tasks) {
      taskInfos.add(task.getInfo());
    }

    return taskInfos;
  }

  private void recordTasks(List<Task> tasks)
    throws ClassNotFoundException, IOException, InterruptedException, ExecutionException {
    for (Task task : tasks) {
      state.recordTask(task);
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
            .addAllVariables(getExecutorEnvironment())).setValue(cmd).build())
      .build();
  }

  private List<Environment.Variable> getExecutorEnvironment() {
    return Arrays.asList(
      createEnvironment("LD_LIBRARY_PATH", config.getLdLibraryPath()),
      createEnvironment("EXECUTOR_OPTS", "-Xmx" + config.getExecutorHeap() + "m -Xms" +
        config.getExecutorHeap() + "m"));
  }

  private Environment.Variable createEnvironment(String key, String value) {
    return Environment.Variable.newBuilder()
      .setName(key)
      .setValue(value).build();
  }

  private List<String> getTaskNames(String taskType) {
    List<String> names = new ArrayList<String>();

    try {
      List<Task> tasks = state.getTasks();
      for (Task task : tasks) {
        if (task.getType().equals(taskType)) {
          names.add(task.getName());
        }
      }
    } catch (Exception ex) {
      log.error("Failed to retrieve task names, with exception: " + ex);
    }

    return names;
  }

  private int getTaskTargetCount(String taskType) throws SchedulerException {
    switch (taskType) {
      case HDFSConstants.NAME_NODE_ID:
        return HDFSConstants.TOTAL_NAME_NODES;
      case HDFSConstants.JOURNAL_NODE_ID:
        return config.getJournalNodeCount();
      default:
        return 0;
    }
  }

  private String getNextTaskName(String taskType) {
    int targetCount = getTaskTargetCount(taskType);
    for (int i = 1; i <= targetCount; i++) {
      Collection<String> nameNodeTaskNames = getTaskNames(taskType);
      String nextName = taskType + i;
      if (!nameNodeTaskNames.contains(nextName)) {
        return nextName;
      }
    }

    // If we are attempting to find a name for a node type that
    // expects more than 1 instance (e.g. namenode1, namenode2, etc.)
    // we should not reach here.
    if (targetCount > 0) {
      String errorStr = "Task name requested when no more names are available for Task type: " + taskType;
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

  protected boolean enoughResources(Offer offer, double cpus, int mem) {
    for (Resource offerResource : offer.getResourcesList()) {
      if (offerResource.getName().equals("cpus") &&
        cpus + config.getExecutorCpus() > offerResource.getScalar().getValue()) {
        return false;
      }

      if (offerResource.getName().equals("mem") &&
        (mem * config.getJvmOverhead())
          + (config.getExecutorHeap() * config.getJvmOverhead())
          > offerResource.getScalar().getValue()) {
        return false;
      }
    }

    return true;
  }

  private List<Task> createTasks(Offer offer) {
    String executorName = getExecutorName();
    String taskIdName = String.format("%s.%s.%d", name, executorName, System.currentTimeMillis());
    List<Task> tasks = new ArrayList<Task>();

    for (String type : getTaskTypes()) {
      List<Resource> resources = resourceFactory.getTaskResources(type);
      ExecutorInfo execInfo = createExecutor(taskIdName, name, executorName);
      String taskName = getNextTaskName(type);

      tasks.add(new Task(resources, execInfo, offer, taskName, type, taskIdName));
    }

    return tasks;
  }
}
