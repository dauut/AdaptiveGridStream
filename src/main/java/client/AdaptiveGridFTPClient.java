package client;


import client.hysterisis.Entry;
import client.hysterisis.Hysterisis;
import client.log.LogManager;
import client.utils.TunableParameters;
import client.utils.Utils;
import client.utils.Utils.Density;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import stork.module.CooperativeModule;
import stork.module.cooperative.GridFTPTransfer;
import stork.util.XferList;

import java.io.*;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

//@SuppressWarnings("Duplicates")
public class AdaptiveGridFTPClient {

    private static final Log LOG = LogFactory.getLog(AdaptiveGridFTPClient.class);
    public static Entry transferTask;
    public static boolean useOnlineTuning = false;
    public static boolean isTransferCompleted = false;
    private TransferAlgorithm algorithm = TransferAlgorithm.MULTICHUNK;
    public static int maximumChunks = 2;
    private int perfFreq = 3;
    public static boolean channelLevelDebug = false;
    private boolean useMaxCC = false;
    private String proxyFile;
    private ChannelDistributionPolicy channelDistPolicy = ChannelDistributionPolicy.ROUND_ROBIN;
    private boolean anonymousTransfer = false;
    private Hysterisis hysterisis;
    private GridFTPTransfer gridFTPClient;
    private boolean useHysterisis = false;
    private boolean useDynamicScheduling = false;
    private boolean runChecksumControl = false;

    //
    private int dataNotChangeCounter = 0;
    private XferList newDataset;
    private HashSet<String> allFiles = new HashSet<>();
    public boolean isNewFile = false;
    private ArrayList<Partition> tmpchunks = null;
    public ArrayList<Partition> chunks;
    private static int dataCheckCounter = 0;
    //    public static int CHANNEL_COUNT = 5;
//    private static int totChanCountGlobal;
    private static int totalChannelCount;
    private boolean firstPassPast = false;
    private static int TRANSFER_NUMBER = 1;
    private List<TunableParameters> staticTunableParams = new ArrayList<>();

    public AdaptiveGridFTPClient() {
        // TODO Auto-generated constructor stub
        //initialize output streams for message logging
        LogManager.createLogFile(ConfigurationParams.STDOUT_ID);
        LogManager.createLogFile(ConfigurationParams.INFO_LOG_ID);
        LogManager.createLogFile(ConfigurationParams.PARAMETERS_LOG);
        transferTask = new Entry();
    }

    @VisibleForTesting
    public AdaptiveGridFTPClient(GridFTPTransfer gridFTPClient) {
        this.gridFTPClient = gridFTPClient;
        LogManager.createLogFile(ConfigurationParams.STDOUT_ID);
        //LogManager.createLogFile(ConfigurationParams.INFO_LOG_ID);
    }

    public static void main(String[] args) throws Exception {
        AdaptiveGridFTPClient multiChunk = new AdaptiveGridFTPClient();
        multiChunk.parseArguments(args, multiChunk);
        multiChunk.lookForNewData();
        multiChunk.firstPassPast = true;
//        multiChunk.streamTransfer();

        Thread streamThread = new Thread(() -> {
            try {
                multiChunk.streamTransfer();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        streamThread.start();

        Thread checkDataPeriodically = new Thread(() -> {
            try {
                multiChunk.checkNewData();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        checkDataPeriodically.start();
        checkDataPeriodically.join();

    }

    private boolean checkNewData() throws InterruptedException {
        dataCheckCounter++;
        System.err.println("Checking new data. Counter = " + dataCheckCounter);
        while (dataNotChangeCounter < 1000) {
            Thread.sleep(10 * 1000); //wait for X sec. before next check
            System.err.println("dataNotChangeCounter: " + dataNotChangeCounter); //number of try.
//            Thread checkData1 = new Thread(this::lookForNewData);
//            checkData1.run();
//            checkData1.join();
//            Thread checkData = new Thread(this::lookForNewData);
//            checkData.start();
//            checkData.join();
            lookForNewData();
            if (isNewFile) {
                dataNotChangeCounter = 0;
                return isNewFile;
            } else {
                dataNotChangeCounter++;
            }
        }
        return isNewFile;
    }


    private XferList lookForNewData() {
        transferTask.setBDP((transferTask.getBandwidth() * transferTask.getRtt()) / 8); // In MB
        LOG.info("*************" + algorithm.name() + "************");

        URI su = null, du = null; //url paths
        try {
            su = new URI(transferTask.getSource()).normalize();
            du = new URI(transferTask.getDestination()).normalize();
        } catch (URISyntaxException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        HostResolution sourceHostResolution = new HostResolution(su.getHost());
        HostResolution destinationHostResolution = new HostResolution(du.getHost());
        sourceHostResolution.start();
        destinationHostResolution.start();

        // create Control Channel to source and destination server
        if (gridFTPClient == null) {
            gridFTPClient = new GridFTPTransfer(proxyFile, su, du);
            gridFTPClient.start();
            gridFTPClient.waitFor();
        }

        //
        if (gridFTPClient == null || GridFTPTransfer.client == null) {
            LOG.info("Could not establish GridFTP connection. Exiting...");
            System.exit(-1);
        }
        gridFTPClient.useDynamicScheduling = useDynamicScheduling;
        gridFTPClient.setPerfFreq(perfFreq);
        GridFTPTransfer.client.setChecksumEnabled(runChecksumControl);

        //Get metadata information of dataset
        XferList dataset = null;

        try {
            // check data
            dataset = gridFTPClient.getListofFiles(su.getPath(), du.getPath(), allFiles);
            //if there is data then cont.
            if (dataset.getFileList().size() == 0) {
                isNewFile = false;
            } else {
                for (int i = 0; i < dataset.getFileList().size(); i++) {
                    if (!allFiles.contains(dataset.getFileList().get(i).fileName)) {
                        allFiles.add(dataset.getFileList().get(i).fileName);
                        isNewFile = true;
                    } else {
                        isNewFile = false;
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        newDataset = dataset; // assign most recent dataset
        if (isNewFile && firstPassPast) {
            try {
                addNewFilesToChunks();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return newDataset;
    }

    private void addNewFilesToChunks() throws Exception {
        boolean staticSettings = false;
        XferList newFiles = newDataset;

        synchronized (chunks.get(0).getRecords()) {
            partitionByFileSize(newFiles, maximumChunks, chunks);
        }

        int[][] estimatedParamsForChunks = new int[chunks.size()][4];
        if(staticSettings){
            for (int i = 0; i < estimatedParamsForChunks.length; i++) {
                chunks.get(i).setTunableParameters(staticTunableParams.get(i));
            }

        }else{
            for (int i = 0; i < estimatedParamsForChunks.length; i++) {
                chunks.get(i).setTunableParameters(Utils.getBestParams(chunks.get(i).getRecords(), maximumChunks));
            }
        }

        LogManager.writeToLog("\nOther rounds of transfer ", ConfigurationParams.PARAMETERS_LOG);
        writeParameterLogs(chunks, estimatedParamsForChunks);

        for (Partition chunk : chunks) {
            LOG.info("NewestChunk :" + chunk.getDensity().name() + " cc:" + chunk.getTunableParameters().getConcurrency() +
                    " p:" + chunk.getTunableParameters().getParallelism() + " ppq:" + chunk.getTunableParameters().getPipelining());
        }

        long datasetSize = newFiles.size();
        long totalDataSize = 0;

        //chunk parameters gathering
        for (int i = 0; i < chunks.size(); i++) {
            XferList xl = chunks.get(i).getRecords();
            totalDataSize += xl.size();
            xl.initialSize = xl.size();
            xl.updateDestinationPaths();
            xl.channels = GridFTPTransfer.TransferChannel.channelPairList;
            chunks.get(i).isReadyToTransfer = true;
        }
        long start, timeSpent = 0;
        if (chunks.get(0).getRecords().size() > totalChannelCount) {
            int[] channelAllocation = allocateChannelsToChunks(chunks, totalChannelCount);
            // Reserve one file for each chunk before initiating channels otherwise
            // pipelining may cause assigning all chunks to one channel.
            List<List<XferList.MlsxEntry>> firstFilesToSend = new ArrayList<>();
            for (int i = 0; i < chunks.size(); i++) {
                List<XferList.MlsxEntry> files = Lists.newArrayListWithCapacity(channelAllocation[i]);
                //setup channels for each chunk
                XferList xl = chunks.get(i).getRecords();
                for (int j = 0; j < channelAllocation[i]; j++) {
                    files.add(xl.pop());
                }
                firstFilesToSend.add(files);
            }

            int currentChannelId = 0;

            start = System.currentTimeMillis();
            for (int i = 0; i < chunks.size(); i++) {
                for (int j = 0; j < channelAllocation[i]; j++) {
                    XferList.MlsxEntry firstFile = synchronizedPop(firstFilesToSend.get(i));
                    boolean success = GridFTPTransfer.setupChannelConf(GridFTPTransfer.TransferChannel.channelPairList.get(j), currentChannelId, chunks.get(i), firstFile);
                    if (success) {
                        GridFTPTransfer.client.transferList(GridFTPTransfer.TransferChannel.channelPairList.get(j));
                    }
                    currentChannelId++;
                }
            }

            timeSpent += ((System.currentTimeMillis() - start) / 1000.0);
            LogManager.writeToLog(algorithm.name() + "\tchunks\t" + maximumChunks + "\tmaxCC\t" +
                            transferTask.getMaxConcurrency() + " Throughput:" + (datasetSize * 8.0) / (timeSpent * (1000.0 * 1000)),
                    ConfigurationParams.INFO_LOG_ID);

            LogManager.writeToLog("TRANSFER NUMBER = " + TRANSFER_NUMBER + " " + algorithm.name() + " chunks: " + maximumChunks + " Throughput:" + "size:" +
                    Utils.printSize(datasetSize, true) + " time:" + timeSpent + " thr: " +
                    (datasetSize * 8.0) / (timeSpent * (1000.0 * 1000)), ConfigurationParams.PARAMETERS_LOG);

            System.out.println("TRANSFER NUMBER = " + TRANSFER_NUMBER + " " + algorithm.name() + " chunks: " + maximumChunks + " Throughput:" + "size:" +
                    Utils.printSize(datasetSize, true) + " time:" + timeSpent + " thr: " +
                    (datasetSize * 8.0) / (timeSpent * (1000.0 * 1000)));
            TRANSFER_NUMBER++;

        }
        checkNewData();
    }

    private void writeParameterLogs(ArrayList<Partition> chunks, int[][] estimatedParamsForChunks) {
        for (int i = 0; i < estimatedParamsForChunks.length; i++) {
            DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
            Date date = new Date();
            LogManager.writeToLog(dateFormat.format(date) + " Parallelism: " + chunks.get(i).getTunableParameters().getParallelism() +
                            "; Pipelining: " + chunks.get(i).getTunableParameters().getPipelining() +
                            "; Concurrency: " + chunks.get(i).getTunableParameters().getConcurrency(),
                    ConfigurationParams.PARAMETERS_LOG);

            chunks.get(i).setTunableParameters(Utils.getBestParams(chunks.get(i).getRecords(), maximumChunks));
        }
    }

    //helper
    public XferList.MlsxEntry synchronizedPop(List<XferList.MlsxEntry> fileList) {
        synchronized (fileList) {
            return fileList.remove(0);
        }
    }

    private void streamTransfer() throws Exception {
        transferTask.setBDP((transferTask.getBandwidth() * transferTask.getRtt()) / 8); // In MB
        LOG.info("*************" + algorithm.name() + "************");

        URI su = null, du = null; //url paths
        try {
            su = new URI(transferTask.getSource()).normalize();
            du = new URI(transferTask.getDestination()).normalize();
        } catch (URISyntaxException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        HostResolution sourceHostResolution = new HostResolution(su.getHost());
        HostResolution destinationHostResolution = new HostResolution(du.getHost());
        sourceHostResolution.start();
        destinationHostResolution.start();

        // create Control Channel to source and destination server
        double startTime = System.currentTimeMillis();
        if (gridFTPClient == null) {
            gridFTPClient = new GridFTPTransfer(proxyFile, su, du);
            gridFTPClient.start();
            gridFTPClient.waitFor();
        }

        //
        if (gridFTPClient == null || GridFTPTransfer.client == null) {
            LOG.info("Could not establish GridFTP connection. Exiting...");
            System.exit(-1);
        }
        gridFTPClient.useDynamicScheduling = useDynamicScheduling;
        gridFTPClient.setPerfFreq(perfFreq);
        GridFTPTransfer.client.setChecksumEnabled(runChecksumControl);

        long datasetSize = newDataset.size();
        // create chunks with new data
        chunks = partitionByFileSize(newDataset, maximumChunks, tmpchunks);
        tmpchunks = chunks;

        LOG.info("Chunk count = " + chunks.size());
        for (int i = 0; i < chunks.size(); i++) {
            LOG.info("Chunk" + i + " file count = " + chunks.get(i).getRecords().getFileList().size());
        }

        // Make sure hostname resolution operations are completed before starting to a transfer
        sourceHostResolution.join();
        destinationHostResolution.join();
        for (InetAddress inetAddress : sourceHostResolution.allIPs) {
            if (inetAddress != null)
                gridFTPClient.sourceIpList.add(inetAddress);
        }
        for (InetAddress inetAddress : destinationHostResolution.allIPs) {
            if (inetAddress != null)
                gridFTPClient.destinationIpList.add(inetAddress);
        }

        int[][] estimatedParamsForChunks = new int[chunks.size()][4];
        long timeSpent = 0;
        long start = System.currentTimeMillis();
        gridFTPClient.startTransferMonitor(transferTask);

        // Make sure total channels count does not exceed total file count
        //int totalChannelCount = Math.min(transferTask.getMaxConcurrency(), newDataset.count());
        totalChannelCount = 10;
//        int totalChannelCount = CHANNEL_COUNT;

        for (int i = 0; i < estimatedParamsForChunks.length; i++) {
            staticTunableParams.add(Utils.getBestParams(chunks.get(i).getRecords(), maximumChunks));
            chunks.get(i).setTunableParameters(Utils.getBestParams(chunks.get(i).getRecords(), maximumChunks));
        }

        LogManager.writeToLog("-----------------------------\n\n" + "First round of transfer ", ConfigurationParams.PARAMETERS_LOG);
        writeParameterLogs(chunks, estimatedParamsForChunks);

        LOG.info(" Running MC with :" + totalChannelCount + " channels.");
        for (Partition chunk : chunks) {
            LOG.info("Chunk :" + chunk.getDensity().name() + " cc:" + chunk.getTunableParameters().getConcurrency() +
                    " p:" + chunk.getTunableParameters().getParallelism() + " ppq:" + chunk.getTunableParameters().getPipelining());
        }
        int[] channelAllocation = allocateChannelsToChunks(chunks, totalChannelCount);
        start = System.currentTimeMillis();

        gridFTPClient.runMultiChunkTransfer(chunks, channelAllocation);

        timeSpent += ((System.currentTimeMillis() - start) / 1000.0);
        LogManager.writeToLog(algorithm.name() + "\tchunks\t" + maximumChunks + "\tmaxCC\t" +
                        transferTask.getMaxConcurrency() + " Throughput:" + (datasetSize * 8.0) / (timeSpent * (1000.0 * 1000)),
                ConfigurationParams.INFO_LOG_ID);
        System.out.println(algorithm.name() + " chunks: " + maximumChunks + " Throughput:" + "size:" +
                Utils.printSize(datasetSize, true) + " time:" + timeSpent + " thr: " +
                (datasetSize * 8.0) / (timeSpent * (1000.0 * 1000)));

        if (dataNotChangeCounter >= 20) {
            isTransferCompleted = true;
            GridFTPTransfer.executor.shutdown();
            while (!GridFTPTransfer.executor.isTerminated()) {
            }
            LogManager.close();
            gridFTPClient.stop();
        }

//        }
    }

/*
    private void closeConnection() throws InterruptedException {
        System.err.println("No new data: Connection closing...");
        Thread.sleep(2000);
        synchronized (this) {
            wait();
            isTransferCompleted = true;
            GridFTPTransfer.executor.shutdown();
            while (!GridFTPTransfer.executor.isTerminated()) {
            }
            LogManager.close();
            gridFTPClient.stop();
        }
    }
*/

    @VisibleForTesting
    ArrayList<Partition> partitionByFileSize(XferList list, int maximumChunks, ArrayList<Partition> currentChunks) {
        Entry transferTask = getTransferTask();
        list.shuffle();

        ArrayList<Partition> partitions;
        // in case of existing chunks we'll add new files to current chunks
        if (currentChunks != null) {
            partitions = currentChunks;
        } else {
            partitions = new ArrayList<>();
            for (int i = 0; i < maximumChunks; i++) {
                Partition p = new Partition();
                partitions.add(p);
            }
        }

        for (XferList.MlsxEntry e : list) {
            if (e.dir) {
                continue;
            }
            Density density = Utils.findDensityOfFile(e.size(), transferTask.getBandwidth(), maximumChunks);
            try {
                partitions.get(density.ordinal()).addRecord(e);
            } catch (IndexOutOfBoundsException ex) {
                //that's not a nice way to add but it works for now
                ex.printStackTrace();
                Partition p2 = new Partition();
                partitions.add(p2);
                partitions.get(density.ordinal()).addRecord(e);
            }
        }

        Collections.sort(partitions);
        mergePartitions(partitions);

        for (int i = 0; i < partitions.size(); i++) {
            Partition chunk = partitions.get(i);
            chunk.getRecords().sp = list.sp;
            chunk.getRecords().dp = list.dp;
            long avgFileSize = chunk.getRecords().size() / chunk.getRecords().count();
            chunk.setEntry(transferTask);
            chunk.entry.setFileSize(avgFileSize);
            chunk.entry.setFileCount(chunk.getRecords().count());
            chunk.entry.setDensity(Entry.findDensityOfList(avgFileSize, transferTask.getBandwidth(), maximumChunks));
            chunk.entry.calculateSpecVector();
            chunk.setDensity(Entry.findDensityOfList(avgFileSize, transferTask.getBandwidth(), maximumChunks));
            LOG.info("Chunk " + i + ":\tfiles:" + partitions.get(i).getRecords().count() + "\t avg:" +
                    Utils.printSize(partitions.get(i).getCentroid(), true)
                    + " \t total:" + Utils.printSize(partitions.get(i).getRecords().size(), true) + " Density:" +
                    chunk.getDensity());
        }

        return partitions;
    }

    private ArrayList<Partition> reOrderChunks(ArrayList<Partition> partitions, int chunkAmount) {
        ArrayList<Partition> newChunk = new ArrayList<>();

        for (int i = 0; i < chunkAmount; i++) {
            Partition p = new Partition();
            newChunk.add(p);
        }

        for (int i = 0; i < partitions.size(); i++) {
            switch (partitions.get(i).getDensity().name()) {
                case "SMALL":
                    newChunk.add(0, partitions.get(i));
                    break;
                case "LARGE":
                    newChunk.add(1, partitions.get(i));
                    break;
                case "MEDIUM":
                    newChunk.add(2, partitions.get(i));
                    break;
                case "HUGE":
                    newChunk.add(3, partitions.get(i));
                    break;
            }
        }

        return newChunk;

    }

    private ArrayList<Partition> mergePartitions(ArrayList<Partition> partitions) {
        for (int i = 0; i < partitions.size(); i++) {
            Partition p = partitions.get(i);

            //merge small chunk with the the chunk with closest centroid
            if ((p.getRecords().count() < 2 || p.getRecords().size() < 5 * transferTask.getBDP()) && partitions.size() > 1) {
                int index = -1;
                LOG.info(i + " is too small " + p.getRecords().count() + " files total size:" + Utils.printSize(p.getRecords().size(), true));
                double diff = Double.POSITIVE_INFINITY;
                for (int j = 0; j < partitions.size(); j++) {
                    if (j != i && Math.abs(p.getCentroid() - partitions.get(j).getCentroid()) < diff) {
                        diff = Math.abs(p.getCentroid() - partitions.get(j).getCentroid());
                        index = j;
                    }
                }
                if (index == -1) {
                    LOG.fatal("Fatal error: Could not find chunk to merge!");
                    System.exit(-1);
                }
                partitions.get(index).getRecords().addAll(p.getRecords());
                LOG.info("Partition " + i + " " + p.getRecords().count() + " files " +
                        Utils.printSize(p.getRecords().size(), true));
                LOG.info("Merging partition " + i + " to partition " + index);
                partitions.remove(i);
                i--;
            }
        }
        return partitions;
    }

    private int[] allocateChannelsToChunks(List<Partition> chunks, final int channelCount) {
        int totalChunks = chunks.size();
        int[] fileCount = new int[totalChunks];
        for (int i = 0; i < totalChunks; i++) {
            fileCount[i] = chunks.get(i).getRecords().count();
        }

        int[] concurrencyLevels = new int[totalChunks];
        if (channelDistPolicy == ChannelDistributionPolicy.ROUND_ROBIN) {
            int modulo = (totalChunks + 1) / 2;
            int count = 0;
            for (int i = 0; count < channelCount; i++) {
                int index = i % modulo;
                if (concurrencyLevels[index] < fileCount[index]) {
                    concurrencyLevels[index]++;
                    count++;
                }
                if (index < totalChunks - index - 1 && count < channelCount
                        && concurrencyLevels[totalChunks - index - 1] < fileCount[totalChunks - index - 1]) {
                    concurrencyLevels[totalChunks - index - 1]++;
                    count++;
                }
            }

            for (int i = 0; i < totalChunks; i++) {
                System.out.println("Chunk " + i + ":" + concurrencyLevels[i] + "channels");
            }
        } else {
            double[] chunkWeights = new double[chunks.size()];
            double totalWeight = 0;
            if (useHysterisis) {
        /*
        int[][] estimatedParams = hysterisis.getEstimatedParams();
        double[] estimatedThroughputs = hysterisis.getEstimatedThroughputs();
        double[] estimatedUnitThroughputs = new double[chunks.size()];
        for (int i = 0; i < chunks.size(); i++) {
          estimatedUnitThroughputs[i] = estimatedThroughputs[i] / estimatedParams[i][0];
          LOG.info("estimated unit thr:" + estimatedUnitThroughputs[i] + " " + estimatedParams[i][0]);
        }
        double totalThroughput = 0;
        for (double estimatedUnitThroughput : estimatedUnitThroughputs) {
          totalThroughput += estimatedUnitThroughput;
        }
        for (int i = 0; i < estimatedThroughputs.length; i++) {
          chunkWeights[i] = chunks.get(i).getTotalSize() * (totalThroughput / estimatedUnitThroughputs[i]);
          totalWeight += chunkWeights[i];
        }
        */
            } else {
                double[] chunkSize = new double[chunks.size()];
                for (int i = 0; i < chunks.size(); i++) {
                    chunkSize[i] = chunks.get(i).getTotalSize();
                    Utils.Density densityOfChunk = chunks.get(i).getDensity();
                    switch (densityOfChunk) {
                        case SMALL:
                            chunkWeights[i] = 3 * chunkSize[i];
                            break;
                        case MEDIUM:
                            chunkWeights[i] = 2 * chunkSize[i];
                            break;
                        case LARGE:
                            chunkWeights[i] = 1 * chunkSize[i];
                            break;
                        case HUGE:
                            chunkWeights[i] = 1 * chunkSize[i];
                            break;
                        default:
                            break;
                    }
                    totalWeight += chunkWeights[i];
                }
            }
            int remainingChannelCount = channelCount;

            for (int i = 0; i < totalChunks; i++) {
                double propChunkWeight = (chunkWeights[i] * 1.0 / totalWeight);
                concurrencyLevels[i] = Math.min(remainingChannelCount, (int) Math.floor(channelCount * propChunkWeight));
                remainingChannelCount -= concurrencyLevels[i];
                //System.out.println("Channel "+i + "weight:" +propChunkWeight  + "got " + concurrencyLevels[i] + "channels");
            }

            // Since we take floor when calculating, total channels might be unassigned.
            // If so, starting from chunks with zero channels, assign remaining channels
            // in round robin fashion
            for (int i = 0; i < chunks.size(); i++) {
                if (concurrencyLevels[i] == 0 && remainingChannelCount > 0) {
                    concurrencyLevels[i]++;
                    remainingChannelCount--;
                }
            }
            //find the chunks with minimum assignedChannelCount
            while (remainingChannelCount > 0) {
                int minChannelCount = Integer.MAX_VALUE;
                int chunkIdWithMinChannel = -1;
                for (int i = 0; i < chunks.size(); i++) {
                    if (concurrencyLevels[i] < minChannelCount) {
                        minChannelCount = concurrencyLevels[i];
                        chunkIdWithMinChannel = i;
                    }
                }
                concurrencyLevels[chunkIdWithMinChannel]++;
                remainingChannelCount--;
            }
            for (int i = 0; i < totalChunks; i++) {
                Partition chunk = chunks.get(i);
                LOG.info("Chunk " + chunk.getDensity() + " weight " + chunkWeights[i] + " cc: " + concurrencyLevels[i]);
            }
        }
        return concurrencyLevels;
    }

    public Entry getTransferTask() {
        return transferTask;
    }

    private void parseArguments(String[] arguments, AdaptiveGridFTPClient multiChunk) {
        ClassLoader classloader = Thread.currentThread().getContextClassLoader();
        String configFile = "config.cfg";
        InputStream is;
        try {
            if (arguments.length > 0) {
                is = new FileInputStream(arguments[0]);
            } else {
                is = classloader.getResourceAsStream(configFile);
            }

            BufferedReader br = new BufferedReader(new InputStreamReader(is));
            String line;
            while ((line = br.readLine()) != null) {
                processParameter(line.split("\\s+")); //resolve the configuration parameters
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        for (int i = 1; i < arguments.length; i++) {
            String argument = arguments[i];
            if (i + 1 < arguments.length) {
                String value = arguments[i + 1];
                if (processParameter(argument, value))
                    i++;
            } else
                processParameter(argument);
        }

        if (null == proxyFile && !anonymousTransfer) {
            int uid = findUserId();
            proxyFile = "/tmp/x509up_u" + uid;
        }
        ConfigurationParams.init(); //init parameters
    }

    private boolean processParameter(String... args) {
        String config = args[0];
        boolean usedSecondArgument = true;
        if (config.startsWith("#")) {
            return !usedSecondArgument;
        }
        switch (config) {
            case "-s":
            case "-source":
                if (args.length > 1) {
                    transferTask.setSource(args[1]);
                } else {
                    LOG.fatal("-source requires source address");
                }
                LOG.info("source  = " + transferTask.getSource());
                break;
            case "-d":
            case "-destination":
                if (args.length > 1) {
                    transferTask.setDestination(args[1]);
                } else {
                    LOG.fatal("-destination requires a destination address");
                }
                LOG.info("destination = " + transferTask.getDestination());
                break;
            case "-proxy":
                if (args.length > 1) {
                    proxyFile = args[1];
                } else {
                    LOG.fatal("-spath requires spath of file/directory to be transferred");
                }
                LOG.info("proxyFile = " + proxyFile);
                break;
            case "-no-proxy":
                anonymousTransfer = true;
                usedSecondArgument = false;
                break;
            case "-bw":
            case "-bandwidth":
                if (args.length > 1 || Double.parseDouble(args[1]) > 100) {
                    transferTask.setBandwidth(Math.pow(10, 9) * Double.parseDouble(args[1]));
                } else {
                    LOG.fatal("-bw requires bandwidth in GB");
                }
                LOG.info("bandwidth = " + transferTask.getBandwidth() + " GB");
                break;
            case "-rtt":
                if (args.length > 1) {
                    transferTask.setRtt(Double.parseDouble(args[1]));
                } else {
                    LOG.fatal("-rtt requires round trip time in millisecond");
                }
                LOG.info("rtt = " + transferTask.getRtt() + " ms");
                break;
            case "-maxcc":
            case "-max-concurrency":
                if (args.length > 1) {
                    transferTask.setMaxConcurrency(Integer.parseInt(args[1]));
                } else {
                    LOG.fatal("-cc needs integer");
                }
                LOG.info("cc = " + transferTask.getMaxConcurrency());
                break;
            case "-bs":
            case "-buffer-size":
                if (args.length > 1) {
                    transferTask.setBufferSize(Double.parseDouble(args[1]) * 1024 * 1024); //in MB
                } else {
                    LOG.fatal("-bs needs integer");
                }
                LOG.info("bs = " + transferTask.getBufferSize());
                break;
            case "-testbed":
                if (args.length > 1) {
                    transferTask.setTestbed(args[1]);
                } else {
                    LOG.fatal("-testbed needs testbed name");
                }
                LOG.info("Testbed name is = " + transferTask.getTestbed());
                break;
            case "-input":
                if (args.length > 1) {
                    ConfigurationParams.INPUT_DIR = args[1];
                } else {
                    LOG.fatal("-historical data input file spath has to be passed");
                }
                LOG.info("Historical data spath = " + ConfigurationParams.INPUT_DIR);
                break;
            case "-maximumChunks":
                if (args.length > 1) {
                    maximumChunks = Integer.parseInt(args[1]);
                } else {
                    LOG.fatal("-maximumChunks requires an integer value");
                }
                LOG.info("Number of chunks = " + maximumChunks);
                break;
            case "-use-hysterisis":
                useHysterisis = true;
                LOG.info("Use hysterisis based approach");
                break;
            case "-single-chunk":
                algorithm = TransferAlgorithm.SINGLECHUNK;
                usedSecondArgument = false;
                LOG.info("Use single chunk transfer approach");
                break;
            case "-channel-distribution-policy":
                if (args.length > 1) {
                    if (args[1].compareTo("roundrobin") == 0) {
                        channelDistPolicy = ChannelDistributionPolicy.ROUND_ROBIN;
                    } else if (args[1].compareTo("weighted") == 0) {
                        channelDistPolicy = ChannelDistributionPolicy.WEIGHTED;
                    } else {
                        LOG.fatal("-channel-distribution-policy can be either \"roundrobin\" or \"weighted\"");
                    }
                } else {
                    LOG.fatal("-channel-distribution-policy has to be specified as \"roundrobin\" or \"weighted\"");
                }
                break;
            case "-use-dynamic-scheduling":
                algorithm = TransferAlgorithm.PROACTIVEMULTICHUNK;
                useDynamicScheduling = true;
                channelDistPolicy = ChannelDistributionPolicy.WEIGHTED;
                usedSecondArgument = false;
                LOG.info("Dynamic scheduling enabled.");
                break;
            case "-use-online-tuning":
                useOnlineTuning = true;
                usedSecondArgument = false;
                LOG.info("Online modelling/tuning enabled.");
                break;
            case "-use-checksum":
                runChecksumControl = true;
                usedSecondArgument = false;
                LOG.info("Checksum enabled.");
                break;
            case "-use-max-cc":
                useMaxCC = true;
                usedSecondArgument = false;
                LOG.info("Use of maximum concurrency enabled.");
                break;
            case "-throughput-log-file":
                ConfigurationParams.INFO_LOG_ID = args[1];
                LOG.info("Dynamic scheduling enabled.");
                break;
            case "-perf-freq":
                perfFreq = Integer.parseInt(args[1]);
                break;
            case "-enable-channel-debug":
                channelLevelDebug = true;
                usedSecondArgument = false;
                break;
            default:
                System.err.println("Unrecognized input parameter " + config);
                System.exit(-1);
        }
        return usedSecondArgument;
    }

    private int findUserId() {
        String userName = null;
        int uid = -1;
        try {
            userName = System.getProperty("user.name");
            String command = "id -u " + userName;
            Process child = Runtime.getRuntime().exec(command);
            BufferedReader stdInput = new BufferedReader(new InputStreamReader(child.getInputStream()));
            String s;
            while ((s = stdInput.readLine()) != null) {
                uid = Integer.parseInt(s);
            }
            stdInput.close();
        } catch (IOException e) {
            System.err.print("Proxy file for user " + userName + " not found!" + e.getMessage());
            System.exit(-1);
        }
        return uid;
    }

    public static class HostResolution extends Thread {
        String hostname;
        InetAddress[] allIPs;

        public HostResolution(String hostname) {
            this.hostname = hostname;
        }

        @Override
        public void run() {
            try {
                System.out.println("Getting IPs for:" + hostname);
                allIPs = InetAddress.getAllByName(hostname);
                System.out.println("Found IPs for:" + hostname);
                //System.out.println("IPs for:" + du.getHost());
                for (int i = 0; i < allIPs.length; i++) {
                    System.out.println(allIPs[i]);
                }
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(-1);
            }
        }

    }

    enum TransferAlgorithm {SINGLECHUNK, MULTICHUNK, PROACTIVEMULTICHUNK}

    private enum ChannelDistributionPolicy {ROUND_ROBIN, WEIGHTED}


}
