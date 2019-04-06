package stork.module.cooperative;

import org.globus.ftp.Buffer;
import org.globus.ftp.DataSink;
import stork.util.XferList;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.util.HashSet;
import java.util.LinkedList;

public class ListSink extends Reader implements DataSink {
    private String base;
    private LinkedList<Buffer> buf_list;
    private Buffer cur_buf = null;
    private BufferedReader br;
    private int off = 0;


    // files in source path
    public ListSink(String base) {
        this.base = base;
        buf_list = new LinkedList<Buffer>();
        br = new BufferedReader(this);
    }

    public void write(Buffer buffer) throws IOException {
        buf_list.add(buffer);
        //System.out.println(new String(buffer.getBuffer()));
    }

    public void close() throws IOException {
    }

    private Buffer nextBuf() {
        try {
            return cur_buf = buf_list.pop();
        } catch (Exception e) {
            return cur_buf = null;
        }
    }

    // Increment reader offset, getting new buffer if needed.
    private void skip(int amt) {
        off += amt;

        // See if we need a new buffer from the list.
        while (cur_buf != null && off >= cur_buf.getLength()) {
            off -= cur_buf.getLength();
            nextBuf();
        }
    }

    // Read some bytes from the reader into a char array.
    public int read(char[] cbuf, int co, int cl) throws IOException {
        if (cur_buf == null && nextBuf() == null) {
            return -1;
        }

        byte[] bbuf = cur_buf.getBuffer();
        int bl = bbuf.length - off;
        int len = (bl < cl) ? bl : cl;

        for (int i = 0; i < len; i++)
            cbuf[co + i] = (char) bbuf[off + i];

        skip(len);

        // If we can write more, write more.
        if (len < cl && cur_buf != null) {
            len += read(cbuf, co + len, cl - len);
        }

        return len;
    }

    // Read a line, updating offset.
    private String readLine() {
        try {
            return br.readLine();
        } catch (Exception e) {
            return null;
        }
    }

    // Get the list from the sink as an XferList.
    public XferList getList(String path, HashSet<String> prevList) {
        XferList xl = new XferList(base, "");
        String line;

        // Read lines from the buffer list.
        while ((line = readLine()) != null) {
            try {
                org.globus.ftp.MlsxEntry m = new org.globus.ftp.MlsxEntry(line);

                String fileName = m.getFileName();
                String type = m.get("type");
                String size = m.get("size");

                // check if we have files previous bulk
                if (prevList != null && !prevList.contains(fileName)) {
                    if (type.equals(org.globus.ftp.MlsxEntry.TYPE_FILE)) {
                        xl.add(path + fileName, Long.parseLong(size));
                    } else if (!fileName.equals(".") && !fileName.equals("..")) {
                        xl.add(path + fileName);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                continue;  // Weird data I guess!
            }
        }
        return xl;
    }


    // new metgod du
    private boolean isExist(XferList prevList, String filename) {
        boolean isExist = false;
        int counter = 0;

        while (counter < prevList.getFileList().size() && !isExist) {

            if (prevList.getFileList().get(counter).fileName.equals(filename)) {
                isExist = true;
                counter++;
            } else {
                counter++;
            }
        }

        return isExist;
    }
}
