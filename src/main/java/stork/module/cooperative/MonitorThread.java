package stork.module.cooperative;

import client.AdaptiveGridFTPClient;
import org.globus.ftp.PerfMarker;
import org.globus.ftp.vanilla.Reply;
import stork.module.CooperativeModule;
import stork.util.TransferProgress;
import stork.util.XferList;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;

public class MonitorThread extends Thread {
  public TransferProgress progress = null;
  public Exception error = null;
  ProgressListener pl = null;
  XferList fileList;
  XferList.MlsxEntry e;
  private ControlChannel cc;
  private MonitorThread other = this;

  public MonitorThread(ControlChannel cc, XferList.MlsxEntry e) {
    this.cc = cc;
    this.e  = e;
  }

  public void pair(MonitorThread other) {
    this.other = other;
    other.other = this;
  }

  public void process() throws Exception {
    Reply r = cc.read();

    if (progress == null) {
      progress = new TransferProgress();
    }

    //ProgressListener pl = new ProgressListener(progress);

    if (other.error != null) {
      throw other.error;
    }

    if (r != null && (!Reply.isPositivePreliminary(r)) ) {
      error = new Exception("failed to start " + r.getCode() + ":" + r.getCategory() + ":" + r.getMessage());
    }
    while (other.error == null) {
      r = cc.read();
      if (r != null) {
        switch (r.getCode()) {
          case 111:  // Restart marker
            break;   // Just ignore for now...
          case 112:  // Progress marker
            if (pl != null) {
            //System.out.println("Progress Marker from " + cc.fc.getHost());
              long diff = pl._markerArrived(new PerfMarker(r.getMessage()), e);
              if (AdaptiveGridFTPClient.channelLevelDebug) {
                if (cc.instantThroughputWriter == null) {
                  System.out.println("Initializing writer of channel " + cc.channelID + " on host" + cc.fc.getHost());
                  cc.instantThroughputWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(
                      "Channel-" + cc.channelID + "-inst-thr.txt"), "utf-8"));
                }
                if (diff > 0) {
                  cc.instantThroughputWriter.write(System.currentTimeMillis() / 1000 + "\t" + diff + "\n");
                  cc.instantThroughputWriter.flush();
                }
              }
              pl.client.updateChunk(fileList, diff);
            }
            break;
          case 125:  // Transfer complete!
            break;
          case 226:  // Transfer complete!
            return;
          case 227:  // Entering passive mode
            return;
          default:
            //System.out.println("Error:" + cc.fc.getHost());
            throw new Exception("unexpected reply: " + r.getCode() + " " + r.getMessage());
        }   // We'd have returned otherwise...
      }
    }
    if (cc.instantThroughputWriter != null)
      cc.instantThroughputWriter.flush();
    throw other.error;
  }

  public void run() {
    try {
      process();
    } catch (Exception e) {
      error = e;
      e.printStackTrace();
      System.exit(-1);
    }
  }
}
