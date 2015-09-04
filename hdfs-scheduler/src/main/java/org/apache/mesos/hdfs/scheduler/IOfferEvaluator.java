package org.apache.mesos.hdfs.scheduler;

import org.apache.mesos.Protos.Offer;

public interface IOfferEvaluator {
  public boolean evaluate(Offer offer);
}

