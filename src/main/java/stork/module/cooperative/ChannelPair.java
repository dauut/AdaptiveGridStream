package stork.module.cooperative;

import client.Partition;
import org.globus.ftp.HostPort;
import org.globus.ftp.HostPort6;
import org.globus.ftp.HostPortList;
import org.globus.ftp.exception.FTPReplyParseException;
import org.globus.ftp.exception.ServerException;
import org.globus.ftp.exception.UnexpectedReplyCodeException;
import org.globus.ftp.vanilla.Command;
import org.globus.ftp.vanilla.Reply;
import stork.module.CooperativeModule;
import stork.util.XferList;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;

// Class for binding a pair of control channels and performing pairwise
// operations on them.
public class ChannelPair {
  //public final FTPURI su, du;
  public final boolean gridftp;
  // File list this channel is transferring
  public Partition chunk, newChunk;
  public boolean isConfigurationChanged = false;
  public boolean enableCheckSum = false;
  public Queue<XferList.MlsxEntry> inTransitFiles = new LinkedList<>();
  public int parallelism = 1, pipelining = 0, trev = 5;
  private char mode = 'S', type = 'A';
  public boolean dc_ready = false;
  public int id;
  //private XferList.MlsxEntry first;
  //private double bytesTransferred = 0;
  //private long timer;
  //private int updateXferListIndex= 0;
  public int doStriping = 0;
  // Remote/other view of control channels.
  // rc is always remote, oc can be either remote or local.
  public ControlChannel rc, oc;
  // Source/dest view of control channels.
  // Either one of these may be local (but not both).
  public ControlChannel sc, dc;

  // Create a control channel pair. TODO: Check if they can talk.
  public ChannelPair(FTPURI su, FTPURI du) {
    //this.su = su; this.du = du;
    gridftp = !su.ftp && !du.ftp;
    try {
      if (su == null || du == null) {
        throw new Error("ChannelPair called with null args");
      }
      if (su.file && du.file) {
        throw new Exception("file-to-file not supported");
      } else if (su.file) {
        rc = dc = new ControlChannel(du);
        oc = sc = new ControlChannel(rc);
      } else if (du.file) {
        rc = sc = new ControlChannel(su);
        oc = dc = new ControlChannel(rc);
      } else {
        rc = dc = new ControlChannel(du);
        oc = sc = new ControlChannel(su);
      }
    } catch (Exception e) {
      System.out.println("Failed to create new channel on " + su.host + "-" + du.host);
      e.printStackTrace();
    }
  }

  // Pair a channel with a new local channel. Note: don't duplicate().
  public ChannelPair(ControlChannel cc) throws Exception {
    if (cc.local) {
      throw new Error("cannot create local pair for local channel");
    }
    //du = null; su = null;
    gridftp = cc.gridftp;
    rc = dc = cc;
    oc = sc = new ControlChannel(cc);
  }

  public void setID (int id) {
    this.id = id;
    sc.channelID = dc.channelID = rc.channelID = oc.channelID = id;
  }

  public void pipePassive() throws Exception {
    rc.write(rc.fc.isIPv6() ? "EPSV" : "PASV");
  }

  // Read and handle the response of a pipelined PASV.
  public HostPort getPasvReply() {
    Reply r = null;
    try {
      r = rc.read();
      //System.out.println("passive reply\t"+r.getMessage());
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    String s = r.getMessage().split("[()]")[1];
    return new HostPort(s);
  }

  public HostPort setPassive() throws Exception {
    pipePassive();
    return getPasvReply();
  }

  // Put the other channel into active mode.
  public void setActive(HostPort hp) throws Exception {
    if (oc.local) {
      //oc.facade
      oc.facade.setActive(hp);
    } else if (oc.fc.isIPv6()) {
      oc.execute("EPRT", hp.toFtpCmdArgument());
    } else {
      oc.execute("PORT", hp.toFtpCmdArgument());
    }
    dc_ready = true;
  }

  public HostPortList setStripedPassive()
      throws IOException,
          ServerException {
    // rc.write(rc.fc.isIPv6() ? "EPSV" : "PASV");
    Command cmd = new Command("SPAS",
        (rc.fc.isIPv6()) ? "2" : null);
    HostPortList hpl;
    Reply reply = null;

    try {
      reply = rc.execute(cmd);
    } catch (UnexpectedReplyCodeException urce) {
      throw ServerException.embedUnexpectedReplyCodeException(urce);
    } catch (FTPReplyParseException rpe) {
      throw ServerException.embedFTPReplyParseException(rpe);
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    //this.gSession.serverMode = GridFTPSession.SERVER_EPAS;
    if (rc.fc.isIPv6()) {
      hpl = HostPortList.parseIPv6Format(reply.getMessage());
      int size = hpl.size();
      for (int i = 0; i < size; i++) {
        HostPort6 hp = (HostPort6) hpl.get(i);
        if (hp.getHost() == null) {
          hp.setVersion(HostPort6.IPv6);
          hp.setHost(rc.fc.getHost());
        }
      }
    } else {
      hpl =
          HostPortList.parseIPv4Format(reply.getMessage());
    }
    return hpl;
  }

  /**
   * 366      * Sets remote server to striped active server mode (SPOR).
   **/
  public void setStripedActive(HostPortList hpl)
      throws IOException,
      ServerException {
    Command cmd = new Command("SPOR", hpl.toFtpCmdArgument());

    try {
      oc.execute(cmd);
    } catch (UnexpectedReplyCodeException urce) {
      throw ServerException.embedUnexpectedReplyCodeException(urce);
    } catch (FTPReplyParseException rpe) {
      throw ServerException.embedFTPReplyParseException(rpe);
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    //this.gSession.serverMode = GridFTPSession.SERVER_EACT;
  }

  // Set the mode and type for the pair.
  void setTypeAndMode(char t, char m) throws Exception {
    if (t > 0 && type != t) {
      type = t;
      sc.type(t);
      dc.type(t);
    }
    if (m > 0 && mode != m) {
      mode = m;
      sc.mode(m);
      dc.mode(m);
    }
  }

  // Set the parallelism for this pair.
  void setParallelism(int p) throws Exception {
    if (!rc.gridftp || parallelism == p) {
      return;
    }
    parallelism = p = (p < 1) ? 1 : p;
    sc.execute("OPTS RETR Parallelism=" + p + "," + p + "," + p + ";");
  }

  // Set the parallelism for this pair.
  void setBufferSize(int bs) throws Exception {
    if (!rc.gridftp) {
      return;
    }
    bs = (bs < 1) ? 16384 : bs;
    Reply reply = sc.exchange("SITE RBUFSZ", String.valueOf(bs));
    boolean succeeded = false;
    if (Reply.isPositiveCompletion(reply)) {
      reply = dc.exchange("SITE SBUFSZ", String.valueOf(bs));
      if (Reply.isPositiveCompletion(reply)) {
        succeeded = true;
      }
    }
    if (!succeeded) {
      reply = sc.exchange("RETRBUFSIZE", String.valueOf(bs));
      if (Reply.isPositiveCompletion(reply)) {
        reply = dc.exchange("STORBUFSIZE", String.valueOf(bs));
        if (Reply.isPositiveCompletion(reply)) {
          succeeded = true;
        }
      }
    }
    if (!succeeded) {
      reply = sc.exchange("SITE RETRBUFSIZE", String.valueOf(bs));
      if (Reply.isPositiveCompletion(reply)) {
        reply = dc.exchange("SITE STORBUFSIZE", String.valueOf(bs));
        if (Reply.isPositiveCompletion(reply)) {
          succeeded = true;
        }
      }
    }
    if (!succeeded) {
      System.out.println("Buffer size set failed!");
    }
  }

  // Set event frequency for this pair.
  void setPerfFreq(int f) throws Exception {
    if (!rc.gridftp || trev == f) return;
    trev = f = (f < 1) ? 1 : f;
    rc.exchange("TREV", "PERF", f);
  }

  // Make a directory on the destination.
  void pipeMkdir(String path) throws Exception {
    if (dc.local) {
      new File(path).mkdir();
    } else {
      dc.write("MKD", path);
    }
  }

  // Prepare the channels to transfer an XferEntry.
  public void pipeTransfer(XferList.MlsxEntry e) {
    try {
      if (e.dir) {
        pipeMkdir(e.dpath());
      } else {
        String checksum = null;
        if (enableCheckSum) {
          checksum = pipeGetCheckSum(e.path());
        }
        System.out.println("Piping " + e.path() + " to " + e.dpath());
        pipeRetr(e.path(), e.off, e.len);
        if (enableCheckSum && checksum != null)
          pipeStorCheckSum(checksum);
        pipeStor(e.dpath(), e.off, e.len);
      }
    } catch (Exception err) {
      err.printStackTrace();
    }
  }

  // Prepare the source to retrieve a file.
  // FIXME: Check for ERET/REST support.
  void pipeRetr(String path, long off, long len) throws Exception {
    if (sc.local) {
      sc.facade.retrieve(new FileMap(path, off, len));
    } else if (len > -1) {
      sc.write("ERET", "P", off, len, path);
    } else {
      if (off > 0) {
        sc.write("REST", off);
      }
      sc.write("RETR", path);
    }
  }

  // Prepare the destination to store a file.
  // FIXME: Check for ESTO/REST support.
  void pipeStor(String path, long off, long len) throws Exception {
    if (dc.local) {
      dc.facade.store(new FileMap(path, off, len));
    } else if (len > -1) {
      dc.write("ESTO", "A", off, path);
    } else {
      if (off > 0) {
        dc.write("REST", off);
      }
      dc.write("STOR", path);
    }
  }

  String pipeGetCheckSum(String path) throws Exception {
    String parameters = String.format("MD5 %d %d %s", 0,-1,path);
    Reply r = sc.exchange("CKSM", parameters);
    if (!Reply.isPositiveCompletion(r)) {
      throw new Exception("Error:" + r.getMessage());
    }
    return r.getMessage();
  }


  void pipeStorCheckSum(String checksum) throws Exception {
    String parameters = String.format("MD5 %s", checksum);
    Reply cksumReply = dc.exchange("SCKS", parameters);
    if( !Reply.isPositiveCompletion(cksumReply) ) {
      throw new ServerException(ServerException.SERVER_REFUSED,
          cksumReply.getMessage());
    }
    return;
  }

  // Watch a transfer as it takes place, intercepting status messages
  // and reporting any errors. Use this for pipelined transfers.
  // TODO: I'm sure this can be done better...
  public void watchTransfer(ProgressListener p, XferList.MlsxEntry e) throws Exception {
    MonitorThread rmt, omt;

    rmt = new MonitorThread(rc, e);
    omt = new MonitorThread(oc, e);

    rmt.pair(omt);
    if (p != null) {
      rmt.pl = p;
      rmt.fileList = chunk.getRecords();
    }

    omt.start();
    rmt.run();
    omt.join();
    if (omt.error != null) {
      throw omt.error;
    }
    if (rmt.error != null) {
      throw rmt.error;
    }
  }

  public void close() {
    try {
      sc.close();
      dc.close();
    } catch (Exception e) { /* who cares */ }
  }

  public void abort() {
    try {
      sc.abort();
      dc.abort();
    } catch (Exception e) { /* who cares */ }
  }
  public int getId () {
    return id;
  }

  @Override
  public String toString() {
    return String.valueOf(id);
  }
}
