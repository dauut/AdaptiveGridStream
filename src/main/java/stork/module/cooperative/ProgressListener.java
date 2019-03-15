package stork.module.cooperative;

import org.globus.ftp.Marker;
import org.globus.ftp.MarkerListener;
import org.globus.ftp.PerfMarker;
import stork.module.CooperativeModule;
import stork.util.TransferProgress;
import stork.util.XferList;

// Listens for markers from GridFTP servers and updates transfer
// progress statistics accordingly.
public class ProgressListener implements MarkerListener {
  public long last_bytes = 0;
  TransferProgress prog;
  CooperativeModule.StorkFTPClient client;

  public ProgressListener(TransferProgress prog) {
    this.prog = prog;
  }

  public ProgressListener(CooperativeModule.StorkFTPClient client) {
    this.client = client;
  }

  // When we've received a marker from the server.
  public void markerArrived(Marker m) {
    if (m instanceof PerfMarker) {
      try {
        PerfMarker pm = (PerfMarker) m;
        long cur_bytes = pm.getStripeBytesTransferred();
        long diff = cur_bytes - last_bytes;

        last_bytes = cur_bytes;
        if (prog != null) {
          prog.done(diff);
        }
      } catch (Exception e) {
        // Couldn't get bytes transferred...
      }
    }
  }

  public long _markerArrived(Marker m, XferList.MlsxEntry mlsxEntry) {
    if (m instanceof PerfMarker) {
      try {
        PerfMarker pm = (PerfMarker) m;
        long cur_bytes = pm.getStripeBytesTransferred();
        long diff = cur_bytes - last_bytes;
        //System.out.println("Progress update :" + mlsxEntry.spath + "\t"  +Utils.printSize(diff, true) + "/" +
        //    Utils.printSize(mlsxEntry.size, true));
        last_bytes = cur_bytes;
        if (prog != null) {
          prog.done(diff);
        }
        return diff;
      } catch (Exception e) {
        // Couldn't get bytes transferred...
        return -1;
      }
    }
    return -1;
  }
}
