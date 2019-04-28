package stork.module.cooperative;

import client.AdaptiveGridFTPClient;
import client.ConfigurationParams;
import client.Partition;
import client.hysterisis.Entry;
import client.hysterisis.Hysterisis;
import client.utils.TunableParameters;
import client.utils.Utils;
import com.google.common.collect.Lists;
import org.globus.ftp.HostPort;
import org.globus.ftp.HostPortList;
import org.gridforum.jgss.ExtendedGSSCredential;
import org.gridforum.jgss.ExtendedGSSManager;
import org.ietf.jgss.GSSCredential;
import stork.module.CooperativeModule;
import stork.module.StorkTransfer;
import stork.util.XferList;

import java.io.*;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.*;

import static client.utils.Utils.getChannels;

// Transfer class
// --------------
public class GridFTPTransfer implements StorkTransfer {
  public static StorkFTPClient client;
  public static ExecutorService executor;
  public static Queue<InetAddress> sourceIpList = new LinkedList<>();
  public static Queue<InetAddress> destinationIpList = new LinkedList<>();
  public static Collection<Future<?>> futures = new LinkedList<>();


  static int fastChunkId = -1, slowChunkId = -1, period = 0;
  static FTPURI su = null, du = null;
  public boolean useDynamicScheduling = false;
  Thread thread = null;
  GSSCredential cred = null;
  URI usu = null, udu = null;
  String proxyFile = null;
  volatile int rv = -1;
  Thread transferMonitorThread;

  static int perfFreq = 3;

  //List<MlsxEntry> firstFilesToSend;
  public GridFTPTransfer(String proxy, URI source, URI dest) {
    proxyFile = proxy;
    usu = source;
    udu = dest;
    executor = Executors.newFixedThreadPool(30);
  }

  public void setPerfFreq (int perfFreq) {
    this.perfFreq = perfFreq;
  }

  public static boolean setupChannelConf(ChannelPair cc,
                                         int channelId,
                                         Partition chunk,
                                         XferList.MlsxEntry firstFileToTransfer) {
    TunableParameters params = chunk.getTunableParameters();
    cc.chunk = chunk;
    try {
      cc.setID(channelId);
      if (params.getParallelism() > 1)
        cc.setParallelism(params.getParallelism());
      cc.pipelining = params.getPipelining();
      cc.setBufferSize(params.getBufferSize());
      System.out.println("Performance perf req " + perfFreq);
      cc.setPerfFreq(perfFreq);
      if (!cc.dc_ready) {
        if (cc.dc.local || !cc.gridftp) {
          cc.setTypeAndMode('I', 'S');
        } else {
          cc.setTypeAndMode('I', 'E');
        }
        if (cc.doStriping == 1) {
          HostPortList hpl = cc.setStripedPassive();
          cc.setStripedActive(hpl);
        } else {
          HostPort hp = cc.setPassive();
          cc.setActive(hp);
        }
      }
      cc.pipeTransfer(firstFileToTransfer);
      cc.inTransitFiles.add(firstFileToTransfer);
    } catch (Exception ex) {
      System.out.println("Failed to setup channel");
      ex.printStackTrace();
      return false;
    }
    return true;
  }

  public void process() throws Exception {
    String in = null;  // Used for better error messages.

    // Check if we were provided a proxy. If so, load it.
    if (usu.getScheme().compareTo("gsiftp") == 0 && proxyFile != null) {
      try {
        File cred_file = new File(proxyFile);
        FileInputStream fis = new FileInputStream(cred_file);
        byte[] cred_bytes = new byte[(int) cred_file.length()];
        fis.read(cred_bytes);
        System.out.println("Setting parameters");
        //GSSManager manager = ExtendedGSSManager.getInstance();
        ExtendedGSSManager gm = (ExtendedGSSManager) ExtendedGSSManager.getInstance();
        cred = gm.createCredential(cred_bytes,
            ExtendedGSSCredential.IMPEXP_OPAQUE,
            GSSCredential.DEFAULT_LIFETIME, null,
            GSSCredential.INITIATE_AND_ACCEPT);
        fis.close();

      } catch (Exception e) {
        fatal("error loading x509 proxy: " + e.getMessage());
      }
    }

    // Attempt to connect to hosts.
    // TODO: Differentiate between temporary errors and fatal errors.
    try {
      in = "src";
      su = new FTPURI(usu, cred);
      in = "dest";
      du = new FTPURI(udu, cred);
    } catch (Exception e) {
      fatal("couldn't connect to " + in + " server: " + e.getMessage());
    }
    // Attempt to connect to hosts.
    // TODO: Differentiate between temporary errors and fatal errors.
    try {
      client = new StorkFTPClient(su, du);
    } catch (Exception e) {
      e.printStackTrace();
      fatal("error connecting: " + e);
    }
    // Check that src and dest match.
    if (su.path.endsWith("/") && du.path.compareTo("/dev/null") == 0) {  //File to memory transfer

    } else if (su.path.endsWith("/") && !du.path.endsWith("/")) {
      fatal("src is a directory, but dest is not");
    }
    System.out.println("Done parameters");
    client.chunks = new LinkedList<>();
  }

  private void abort() {
    if (client != null) {
      try {
        client.abort();
      } catch (Exception e) {
      }
    }

    close();
  }

  private void close() {
    try {
      for (ChannelPair cc : client.ccs) {
        cc.close();
      }
    } catch (Exception e) {
    }
  }

  public void run() {
    try {
      process();
      rv = 0;
    } catch (Exception e) {
      CooperativeModule.LOG.warn("Client could not be establieshed. Exiting...");
      e.printStackTrace();
      System.exit(-1);
    }

  }

  public void fatal(String m) throws Exception {
    rv = 255;
    throw new Exception(m);
  }

  public void error(String m) throws Exception {
    rv = 1;
    throw new Exception(m);
  }

  public void start() {
    thread = new Thread(this);
    thread.start();
  }

  public void stop() {
    abort();
    //sink.close();
    close();
  }

  public int waitFor() {
    if (thread != null) {
      try {
        thread.join();
      } catch (Exception e) {
      }
    }

    return (rv >= 0) ? rv : 255;
  }

  public XferList getListofFiles(String sp, String dp, HashSet<String> prevList) throws Exception {
    return client.getListofFiles(sp, dp,prevList);
  }
/*
  public double runTransfer(final Partition chunk) {

    // Set full destination spath of files
    client.chunks.add(chunk);
    XferList xl = chunk.getRecords();
    TunableParameters tunableParameters = chunk.getTunableParameters();
    System.out.println("Transferring chunk " + chunk.getDensity().name() + " params:" + tunableParameters.toString() + " size:" + (xl.size() / (1024.0 * 1024))
        + " files:" + xl.count());
    CooperativeModule.LOG.info("Transferring chunk " + chunk.getDensity().name() + " params:" + tunableParameters.toString() + " " + tunableParameters.getBufferSize() + " size:" + (xl.size() / (1024.0 * 1024))
        + " files:" + xl.count());
    double fileSize = xl.size();
    xl.updateDestinationPaths();

    xl.channels = new LinkedList<>();
    xl.initialSize = xl.size();

    // Reserve one file for each channel, otherwise pipelining
    // may lead to assigning all files to one channel
    int concurrency = tunableParameters.getConcurrency();
    List<XferList.MlsxEntry> firstFilesToSend = Lists.newArrayListWithCapacity(concurrency);
    for (int i = 0; i < concurrency; i++) {
      firstFilesToSend.add(xl.pop());
    }

    client.ccs = new ArrayList<>(concurrency);
    long init = System.currentTimeMillis();


    for (int i = 0; i < concurrency; i++) {
      XferList.MlsxEntry firstFile = synchronizedPop(firstFilesToSend);
      Runnable transferChannel = new TransferChannel(chunk, i, firstFile);
      futures.add(executor.submit(transferChannel));
    }

    // If not all of the files in firstFilsToSend list is used for any reason,
    // move files back to original xferlist xl.
    if (!firstFilesToSend.isEmpty()) {
      CooperativeModule.LOG.info("firstFilesToSend list has still " + firstFilesToSend.size() + "files!");
      synchronized (this) {
        for (XferList.MlsxEntry e : firstFilesToSend)
          xl.addEntry(e);
      }
    }
    try {
      while (chunk.getRecords().totalTransferredSize < chunk.getRecords().initialSize) {
        Thread.sleep(100);
      }
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }

    double timeSpent = (System.currentTimeMillis() - init) / 1000.0;
    double throughputInMb = (fileSize * 8 / timeSpent) / (1000 * 1000);
    double throughput = (fileSize * 8) / timeSpent;
    CooperativeModule.LOG.info("Time spent:" + timeSpent + " chunk size:" + Utils.printSize(fileSize, true) +
        " cc:" + client.getChannelCount() +
        " Throughput:" + throughputInMb);
    for (int i = 1; i < client.ccs.size(); i++) {
      client.ccs.get(i).close();
    }
    client.ccs.clear();
    return throughput;
  }
  */

  public XferList.MlsxEntry synchronizedPop(List<XferList.MlsxEntry> fileList) {
    synchronized (fileList) {
      return fileList.remove(0);
    }
  }

  public void runMultiChunkTransfer(List<Partition> chunks, int[] channelAllocations) throws Exception {
    int totalChannels = 0;

    //channel allocation
    for (int channelAllocation : channelAllocations)
      totalChannels += channelAllocation;
    int totalChunks = chunks.size();

    long totalDataSize = 0;
    for (int i = 0; i < totalChunks; i++) {
      XferList xl = chunks.get(i).getRecords();
      totalDataSize += xl.size();
      xl.initialSize = xl.size();
      xl.updateDestinationPaths();
      xl.channels = Lists.newArrayListWithCapacity(channelAllocations[i]);
      chunks.get(i).isReadyToTransfer = true;
      client.chunks.add(chunks.get(i));  //add chunks for transferring
    }

    // Reserve one file for each chunk before initiating channels otherwise
    // pipelining may cause assigning all chunks to one channel.
    List<List<XferList.MlsxEntry>> firstFilesToSend = new ArrayList<>();
    for (int i = 0; i < totalChunks; i++) {
      List<XferList.MlsxEntry> files = Lists.newArrayListWithCapacity(channelAllocations[i]);
      //setup channels for each chunk
      XferList xl = chunks.get(i).getRecords();
      for (int j = 0; j < channelAllocations[i]; j++) {
        files.add(xl.pop());
      }
      firstFilesToSend.add(files);
    }
    client.ccs = new ArrayList<>(totalChannels);
    int currentChannelId = 0;
    long start = System.currentTimeMillis();
    for (int i = 0; i < totalChunks; i++) {
      CooperativeModule.LOG.info(channelAllocations[i] + " channels will be created for chunk " + i);
      for (int j = 0; j < channelAllocations[i]; j++) {
        XferList.MlsxEntry firstFile = synchronizedPop(firstFilesToSend.get(i));
        Runnable transferChannel = new TransferChannel(chunks.get(i), currentChannelId, firstFile);
        currentChannelId++;
        futures.add(executor.submit(transferChannel));
      }
    }
    CooperativeModule.LOG.info("Created "  + client.ccs.size() + "channels");
    //this is monitoring thread which measures throughput of each chunk in every 3 seconds
    //executor.submit(new TransferMonitor());
    for (Future<?> future : futures) {
      future.get();
    }

    long finish = System.currentTimeMillis();
    double thr = totalDataSize * 8 / ((finish - start) / 1000.0);
    CooperativeModule.LOG.info(" Time:" + ((finish - start) / 1000.0) + " sec Thr:" + (thr / (1000 * 1000)));
    // Close channels
    futures.clear();
    client.ccs.forEach(cp -> cp.close());
    client.ccs.clear();
  }

  private void initializeMonitoring() {
    for (int i = 0; i < client.chunks.size(); i++) {
      if (client.chunks.get(i).isReadyToTransfer) {
        XferList xl = client.chunks.get(i).getRecords();
        CooperativeModule.LOG.info("Chunk " + i + ":\t" + xl.count() + " files\t" + Utils.printSize(xl.size(), true));
        System.out.println("Chunk " + i + ":\t" + xl.count() + " files\t" + Utils.printSize(xl.size(), true)
            + client.chunks.get(i).getTunableParameters());
        xl.instantTransferredSize = xl.totalTransferredSize;
      }
    }
  }

  public void startTransferMonitor(Entry e) {
    if (transferMonitorThread == null || !transferMonitorThread.isAlive()) {
      transferMonitorThread = new Thread(new TransferMonitor(e));
      transferMonitorThread.start();
    }
  }

  private void monitorChannels(int interval, Writer writer, int timer) throws IOException {
    DecimalFormat df = new DecimalFormat("###.##");
    double[] estimatedCompletionTimes = new double[client.chunks.size()];
    for (int i = 0; i < client.chunks.size(); i++) {
      double estimatedCompletionTime = -1;
      Partition chunk = client.chunks.get(i);
      XferList xl = chunk.getRecords();
      double throughputInMbps = 8 * (xl.totalTransferredSize - xl.instantTransferredSize) / (xl.interval + interval);

      if (throughputInMbps == 0) {
        if (xl.totalTransferredSize == xl.initialSize) { // This chunk has finished
          xl.weighted_throughput = 0;
        } else if (xl.weighted_throughput != 0) { // This chunk is running but current file has not been transferred
          //xl.instant_throughput = 0;
          estimatedCompletionTime = ((xl.initialSize - xl.totalTransferredSize) / xl.weighted_throughput) - xl.interval;
          xl.interval += interval;
          System.out.println("Chunk " + i + "\t threads:" + xl.channels.size() + "\t count:" + xl.count() +
              "\t total:" + Utils.printSize(xl.size(), true) + "\t interval:" + xl.interval + "\t onAir:" + xl.onAir);
        } else { // This chunk is active but has not transferred any data yet
          System.out.println("Chunk " + i + "\t threads:" + xl.channels.size() + "\t count:" + xl.count() + "\t total:" + Utils.printSize(xl.size(), true)
              + "\t onAir:" + xl.onAir);
          if (xl.channels.size() == 0) {
            estimatedCompletionTime = Double.POSITIVE_INFINITY;
          } else {
            xl.interval += interval;
          }
        }
      } else {
        xl.instant_throughput = throughputInMbps;
        xl.interval = 0;
        if (xl.weighted_throughput == 0) {
          xl.weighted_throughput = throughputInMbps;
        } else {
          xl.weighted_throughput = xl.weighted_throughput * 0.6 + xl.instant_throughput * 0.4;
        }

        if (AdaptiveGridFTPClient.useOnlineTuning) {
          ModellingThread.jobQueue.add(new ModellingThread.ModellingJob(
              chunk, chunk.getTunableParameters(), xl.instant_throughput));
        }
        estimatedCompletionTime = 8 * (xl.initialSize - xl.totalTransferredSize) / xl.weighted_throughput;
        xl.estimatedFinishTime = estimatedCompletionTime;
        System.out.println("Chunk " + i + "\t threads:" + xl.channels.size() + "\t count:" + xl.count() + "\t finished:"
            + Utils.printSize(xl.totalTransferredSize, true) + "/" + Utils.printSize(xl.initialSize, true)
            + "\t throughput:" +  Utils.printSize(xl.instant_throughput, false) + "/" +
            Utils.printSize(xl.weighted_throughput, true) + "\testimated time:" +
            df.format(estimatedCompletionTime) + "\t onAir:" + xl.onAir);
        xl.instantTransferredSize = xl.totalTransferredSize;
      }
      estimatedCompletionTimes[i] = estimatedCompletionTime;
      writer.write(timer + "\t" + xl.channels.size() + "\t" + (throughputInMbps)/(1000*1000.0) + "\n");
      writer.flush();
    }
    System.out.println("*******************");
    if (client.chunks.size() > 1 && useDynamicScheduling) {
      checkIfChannelReallocationRequired(estimatedCompletionTimes);
    }
  }

  public void checkIfChannelReallocationRequired(double[] estimatedCompletionTimes) {

    // if any channel reallocation is ongoing, then don't go for another!
    for (ChannelPair cp : client.ccs) {
      if (cp.isConfigurationChanged) {
        return;
      }
    }
    List<Integer> blacklist = Lists.newArrayListWithCapacity(client.chunks.size());
    int curSlowChunkId, curFastChunkId;
    while (true) {
      double maxDuration = Double.NEGATIVE_INFINITY;
      double minDuration = Double.POSITIVE_INFINITY;
      curSlowChunkId = -1;
      curFastChunkId = -1;
      for (int i = 0; i < estimatedCompletionTimes.length; i++) {
        if (estimatedCompletionTimes[i] == -1 || blacklist.contains(i)) {
          continue;
        }
        if (estimatedCompletionTimes[i] > maxDuration && client.chunks.get(i).getRecords().count() > 0) {
          maxDuration = estimatedCompletionTimes[i];
          curSlowChunkId = i;
        }
        if (estimatedCompletionTimes[i] < minDuration && client.chunks.get(i).getRecords().channels.size() > 1) {
          minDuration = estimatedCompletionTimes[i];
          curFastChunkId = i;
        }
      }
      System.out.println("CurrentSlow:" + curSlowChunkId + " CurrentFast:" + curFastChunkId +
          " PrevSlow:" + slowChunkId + " PrevFast:" + fastChunkId + " Period:" + (period + 1));
      if (curSlowChunkId == -1 || curFastChunkId == -1 || curSlowChunkId == curFastChunkId) {
        for (int i = 0; i < estimatedCompletionTimes.length; i++) {
          System.out.println("Estimated time of :" + i + " " + estimatedCompletionTimes[i]);
        }
        break;
      }
      XferList slowChunk = client.chunks.get(curSlowChunkId).getRecords();
      XferList fastChunk = client.chunks.get(curFastChunkId).getRecords();
      period++;
      double slowChunkFinishTime = Double.MAX_VALUE;
      if (slowChunk.channels.size() > 0) {
        slowChunkFinishTime = slowChunk.estimatedFinishTime * slowChunk.channels.size() / (slowChunk.channels.size() + 1);
      }
      double fastChunkFinishTime = fastChunk.estimatedFinishTime * fastChunk.channels.size() / (fastChunk.channels.size() - 1);
      if (period >= 3 && (curSlowChunkId == slowChunkId || curFastChunkId == fastChunkId)) {
        if (slowChunkFinishTime >= fastChunkFinishTime * 2) {
          //System.out.println("total chunks  " + client.ccs.size());
          synchronized (fastChunk) {
            ChannelPair transferringChannel = fastChunk.channels.get(fastChunk.channels.size() - 1);
            transferringChannel.newChunk = client.chunks.get(curSlowChunkId);
            transferringChannel.isConfigurationChanged = true;
            System.out.println("Chunk " + curFastChunkId + "*" + getChannels(fastChunk) +  " is giving channel " +
                transferringChannel.id + " to chunk " + curSlowChunkId + "*" + getChannels(slowChunk));
          }
          period = 0;
          break;
        } else {
          if (slowChunk.channels.size() > fastChunk.channels.size()) {
            blacklist.add(curFastChunkId);
          } else {
            blacklist.add(curSlowChunkId);
          }
          System.out.println("Blacklisted chunk " + blacklist.get(blacklist.size() - 1));
        }
      } else if (curSlowChunkId != slowChunkId && curFastChunkId != fastChunkId) {
        period = 1;
        break;
      } else if (period < 3) {
        break;
      }
    }
    fastChunkId = curFastChunkId;
    slowChunkId = curSlowChunkId;

  }



  public static class TransferChannel implements Runnable {
    final int doStriping;
    int channelId;
    XferList.MlsxEntry firstFileToTransfer;
    Partition chunk;
    //List<String> blacklistedHosts

    public TransferChannel(Partition chunk, int channelId, XferList.MlsxEntry file) {
      this.channelId = channelId;
      this.doStriping = 0;
      this.chunk = chunk;
      firstFileToTransfer = file;
    }

    @Override
    public void run() {
      boolean success = false;
      int trial = 0;
      while (!success && trial < 3) {
        try {
          // Channel zero is main channel and already created
          ChannelPair channel;
          InetAddress srcIp, dstIp;
          synchronized (sourceIpList) {
            srcIp = sourceIpList.poll();
            while(srcIp.getCanonicalHostName().contains("ie04")) {
              srcIp = sourceIpList.poll();
            }
            sourceIpList.add(srcIp);
          }
          synchronized (destinationIpList) {
            dstIp = destinationIpList.poll();
            while(dstIp.getCanonicalHostName().contains("ie04")) {
              dstIp = destinationIpList.poll();
            }
            destinationIpList.add(dstIp);
          }
          //long start = System.currentTimeMillis();
          URI srcUri = null, dstUri = null;
          try {
            srcUri = new URI(su.uri.getScheme(), su.uri.getUserInfo(), srcIp.getCanonicalHostName(), su.uri.getPort(),
                su.uri.getPath(), su.uri.getQuery(), su.uri.getFragment());
            dstUri = new URI(du.uri.getScheme(), du.uri.getUserInfo(), dstIp.getCanonicalHostName(), du.uri.getPort(),
                du.uri.getPath(), du.uri.getQuery(), du.uri.getFragment());
          } catch (URISyntaxException e) {
            CooperativeModule.LOG.error("Updating URI host failed:", e);
            System.exit(-1);
          }
          FTPURI srcFTPUri = new FTPURI(srcUri, su.cred);
          FTPURI dstFTPUri = new FTPURI(dstUri, du.cred);
          //System.out.println("Took " + (System.currentTimeMillis() - start)/1000.0 + " seconds to get cannocical name");
          channel = new ChannelPair(srcFTPUri, dstFTPUri);
          //System.out.println("Created a channel between " + channel.rc.fc.getHost() + " and " + channel.sc.fc.getHost());
          success = setupChannelConf(channel, channelId, chunk, firstFileToTransfer);
          if (success) {
            synchronized (chunk.getRecords().channels) {
              chunk.getRecords().channels.add(channel);
            }
            synchronized (client.ccs) {
              client.ccs.add(channel);
            }
            client.transferList(channel);
          } else {
            trial++;

          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
      // if channel is not established, then put the file back into the list
      if (!success) {
        synchronized (chunk.getRecords()) {
          chunk.getRecords().addEntry(firstFileToTransfer);
        }
      }
    }

  }

  public static class ModellingThread implements Runnable {
    public static Queue<ModellingThread.ModellingJob> jobQueue;
    private final int pastLimit = 4;
    public ModellingThread() {
      jobQueue = new ConcurrentLinkedQueue<>();
    }

    @Override
    public void run() {
      while (!AdaptiveGridFTPClient.isTransferCompleted) {
        if (jobQueue.isEmpty()) {
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
          continue;
        }
        ModellingThread.ModellingJob job = jobQueue.peek();
        Partition chunk = job.chunk;

        // If chunk is almost finished, don't update parameters as no gain will be achieved
        XferList xl = chunk.getRecords();
        if(xl.totalTransferredSize >= 0.9 * xl.initialSize || xl.count() <= 2) {
          return;
        }

        TunableParameters tunableParametersUsed = job.tunableParameters;
        double sampleThroughput = job.sampleThroughput;
        double[] params = Hysterisis.runModelling(chunk, tunableParametersUsed, sampleThroughput,
            new double[]{ConfigurationParams.cc_rate, ConfigurationParams.p_rate, ConfigurationParams.ppq_rate});
        TunableParameters tunableParametersEstimated = new TunableParameters.Builder()
            .setConcurrency((int) params[0])
            .setParallelism((int) params[1])
            .setPipelining((int) params[2])
            .setBufferSize((int) AdaptiveGridFTPClient.transferTask.getBufferSize())
            .build();

        chunk.addToTimeSeries(tunableParametersEstimated, params[params.length-1]);
        System.out.println("New round of " + " estimated params: " + tunableParametersEstimated.toString() + " count:" + chunk.getCountOfSeries());
        jobQueue.remove();
        checkForParameterUpdate(chunk, tunableParametersUsed);
      }
      System.out.println("Leaving modelling thread...");
    }

    void checkForParameterUpdate(Partition chunk, TunableParameters currentTunableParameters) {
      // See if previous changes has applied yet
      //if (chunk.getRecords().channels.size() != chunk.getTunableParameters().getConcurrency()) {
      //  return;
      //}
      /*
      for (ChannelPair channel : chunk.getRecords().channels) {
        if (channel.parallelism != chunk.getTunableParameters().getParallelism()) {
          chunk.popFromSeries(); // Dont insert latest probing as it was collected during transition phase
          System.out.println("Channel " + channel.getId() + " P:" + channel.parallelism + " chunkP:" + chunk.getTunableParameters().getParallelism());
          return;
        }
      }
      */

      List<TunableParameters> lastNEstimations = chunk.getLastNFromSeries(pastLimit);
      // If this is first estimation, use it only; otherwise make sure to have pastLimit items
      if (lastNEstimations.size() != 3 && lastNEstimations.size() < pastLimit) {
        return;
      }

      int pastLimit = lastNEstimations.size();
      int ccs[] =  new int[pastLimit];
      int ps[] =  new int[pastLimit];
      int ppqs[] =  new int[pastLimit];
      for (int i = 0; i < pastLimit; i++) {
        ccs[i] = lastNEstimations.get(i).getConcurrency();
        ps[i] = lastNEstimations.get(i).getParallelism();
        ppqs[i] = lastNEstimations.get(i).getPipelining();
      }
      int currentConcurrency = currentTunableParameters.getConcurrency();
      int currentParallelism = currentTunableParameters.getParallelism();
      int currentPipelining = currentTunableParameters.getPipelining();
      int newConcurrency = getUpdatedParameterValue(ccs, currentTunableParameters.getConcurrency());
      int newParallelism = getUpdatedParameterValue(ps, currentTunableParameters.getParallelism());
      int newPipelining = getUpdatedParameterValue(ppqs, currentTunableParameters.getPipelining());
      System.out.println("New parameters estimated:\t" + newConcurrency + "-" + newParallelism + "-" + newPipelining );

      if (newPipelining != currentPipelining) {
        System.out.println("New pipelining " + newPipelining );
        chunk.getRecords().channels.forEach(channel -> channel.pipelining = newPipelining);
        chunk.getTunableParameters().setPipelining(newPipelining);
      }

      if (Math.abs(newParallelism - currentParallelism) >= 2 ||
          Math.max(newParallelism, currentParallelism) >= 2 * Math.min(newParallelism, currentParallelism))  {
        System.out.println("New parallelism " + newParallelism );
        for (ChannelPair channel : chunk.getRecords().channels) {
          channel.isConfigurationChanged = true;
          channel.newChunk = chunk;
        }
        chunk.getTunableParameters().setParallelism(newParallelism);
        chunk.clearTimeSeries();
      }
      if (Math.abs(newConcurrency - currentConcurrency) >= 2) {
        System.out.println("New concurrency " + newConcurrency);
        if (newConcurrency > currentConcurrency) {
          int channelCountToAdd = newConcurrency - currentConcurrency;
          for (int i = 0; i < chunk.getRecords().channels.size(); i++) {
            if (chunk.getRecords().channels.get(i).isConfigurationChanged &&
                chunk.getRecords().channels.get(i).newChunk == null) {
              chunk.getRecords().channels.get(i).isConfigurationChanged = false;
              System.out.println("Cancelled closing of channel " + i);
              channelCountToAdd--;
            }
          }
          while (channelCountToAdd > 0) {
            XferList.MlsxEntry firstFile;
            synchronized (chunk.getRecords()) {
              firstFile = chunk.getRecords().pop();
            }
            if (firstFile != null) {
              TransferChannel transferChannel = new TransferChannel(chunk,
                  chunk.getRecords().channels.size() + channelCountToAdd, firstFile);
              futures.add(executor.submit(transferChannel));
              channelCountToAdd--;
            }
          }
          System.out.println("New concurrency level became " + (newConcurrency - channelCountToAdd));
          chunk.getTunableParameters().setConcurrency(newConcurrency - channelCountToAdd);
        }
        else {
          int randMax = chunk.getRecords().channels.size();
          for (int i = 0; i < currentConcurrency - newConcurrency; i++) {
            int random = ThreadLocalRandom.current().nextInt(0, randMax--);
            chunk.getRecords().channels.get(random).isConfigurationChanged = true;
            chunk.getRecords().channels.get(random).newChunk = null; // New chunk null means closing channel;
            System.out.println("Will close of channel " + random);
          }
          chunk.getTunableParameters().setConcurrency(newConcurrency);
        }
        chunk.clearTimeSeries();
      }
    }

    int getUpdatedParameterValue (int []pastValues, int currentValue) {
      System.out.println("Past values " + currentValue + ", "+ Arrays.toString(pastValues));

      boolean isLarger = pastValues[0] > currentValue;
      boolean isAllLargeOrSmall = true;
      for (int i = 0; i < pastValues.length; i++) {
        if ((isLarger && pastValues[i] <= currentValue) ||
            (!isLarger && pastValues[i] >= currentValue)) {
          isAllLargeOrSmall = false;
          break;
        }
      }

      if (isAllLargeOrSmall) {
        int sum = 0;
        for (int i = 0; i< pastValues.length; i++) {
          sum += pastValues[i];
        }
        System.out.println("Sum: " + sum + " length " + pastValues.length);
        return (int)Math.round(sum/(1.0 * pastValues.length));
      }
      return currentValue;
    }

    public static class ModellingJob {
      private final Partition chunk;
      private final TunableParameters tunableParameters;
      private final double sampleThroughput;

      public ModellingJob (Partition chunk, TunableParameters tunableParameters, double sampleThroughput) {
        this.chunk = chunk;
        this.tunableParameters = tunableParameters;
        this.sampleThroughput = sampleThroughput;
      }
    }
  }

  public class TransferMonitor implements Runnable {
    final int interval = 1000;
    int timer = 0;
    Writer writer;
    Entry dataEntry;
    public TransferMonitor(Entry e){
      dataEntry = e;
    }
    public TransferMonitor(){

    }
    private String getServerName(String st){
      if(st.toLowerCase().contains("stampede2")){
        return "stampede2";
      }else if(st.toLowerCase().contains("oasis-dm.sdsc")){
        return "comet";
      }else if(st.toLowerCase().contains("bridges.psc")){
        return "psc";
      }else if(st.toLowerCase().contains("stampede")){
        return "stampede";
      }else if(st.toLowerCase().contains("lsu")){
        return "supermic";
      }
      else{
        return "iu-wrangler";
      }
    }
    @Override
    public void run() {
      try {
        String file_size = "1600";
        if (dataEntry.getSource().contains("100MB")){
          file_size = "160";
        }else if (dataEntry.getSource().contains("1GB")){
          file_size = "16";
        }else if (dataEntry.getSource().contains("1MB")){
          file_size = "1600";
        }
        String filename = getServerName(dataEntry.getSource())+"-"+getServerName(dataEntry.getDestination())+"-"+file_size+"-"+System.currentTimeMillis()+".log";
        if(getServerName(dataEntry.getSource()).equalsIgnoreCase("comet") ||
                getServerName(dataEntry.getDestination()).equalsIgnoreCase("comet")){
          filename = "logs/comet-transfer/" + filename;
        }else{
          filename = "logs/inst-throughput/" + filename;
        }
        writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filename), "utf-8"));
        initializeMonitoring();
        Thread.sleep(interval);
        while (!AdaptiveGridFTPClient.isTransferCompleted) {
          timer += interval / 1000;
          monitorChannels(interval / 1000, writer, timer);
          Thread.sleep(interval);
        }
        System.out.println("Leaving monitoring...");
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}
