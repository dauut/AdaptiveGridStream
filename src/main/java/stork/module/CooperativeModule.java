package stork.module;

import client.Partition;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.globus.ftp.*;
import org.globus.ftp.vanilla.*;
import stork.module.cooperative.*;
import stork.util.AdSink;
import stork.util.StorkUtil;
import stork.util.TransferProgress;
import stork.util.XferList;

import java.util.*;

@SuppressWarnings("Duplicates")
public class CooperativeModule {

  public static final Log LOG = LogFactory.getLog(CooperativeModule.class);

  private static String MODULE_NAME = "Stork GridFTP Module";
  private static String MODULE_VERSION = "0.1";

  // A sink meant to receive MLSD lists. It contains a list of
  // JGlobus Buffers (byte buffers with offsets) that it reads
  // through sequentially using a BufferedReader to read lines
  // and parse data returned by FTP and GridFTP MLSD commands.


  //  static class Block {
//    long off, len;
//    int para = 0, pipe = 0, conc = 0;
//    double tp = 0;  // Throughput - filled out by caller
//
//    Block(long o, long l) {
//      off = o;
//      len = l;
//    }
//
//    public String toString() {
//      return String.format("<off=%d, len=%d | sc=%d, tp=%.2f>", off, len, para, tp);
//    }
//  }


}