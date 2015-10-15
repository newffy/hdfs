package org.apache.mesos.protobuf;

import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class OfferUtil {

  private final Logger logger = LoggerFactory.getLogger(getClass());

  public static Protos.Offer createOffer(Protos.FrameworkID frameworkID,
    Protos.OfferID offerID, Protos.SlaveID slaveID, String hostname) {
    return Protos.Offer.newBuilder()
      .setId(offerID)
      .setFrameworkId(frameworkID)
      .setSlaveId(slaveID)
      .setHostname(hostname)
      .build();
  }

  public static Protos.Offer createOffer(String frameworkID, String offerID, String slaveID, String hostname) {
    return OfferUtil.createOffer(FrameworkInfoUtil.createFrameworkId(frameworkID),
      OfferUtil.createOfferID(offerID), SlaveUtil.createSlaveID(slaveID), hostname);
  }

  public static Protos.OfferID createOfferID(String offerID) {
    return Protos.OfferID.newBuilder().setValue(offerID).build();
  }
}
