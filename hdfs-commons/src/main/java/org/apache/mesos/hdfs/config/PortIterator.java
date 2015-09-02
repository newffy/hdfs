package org.apache.mesos.hdfs.config;

import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.Value.Range;
import org.apache.mesos.Protos.Value.Ranges;

import java.util.List;

/**
 * Port iterator.
 */
public class PortIterator {
  private List<Range> portRanges;
  private int rangeIndex = 0;
  private long port;

  public PortIterator(Offer offer) {
    for(Resource r: offer.getResourcesList()) {
      if(r.getName().equals("ports")) {
        portRanges = r.getRanges().getRangeList();
      }
    }

    port = portRanges.get(rangeIndex).getBegin();
  }

  public long getNextPort() throws Exception{
    long currPort = port;
    port++;

    if (port > portRanges.get(rangeIndex).getEnd()) {
      rangeIndex++;
      port = portRanges.get(rangeIndex).getBegin();

      if (rangeIndex >= portRanges.size()) {
        throw new Exception("No more ports available.");
      }
    }

    return currPort;
  }
}
