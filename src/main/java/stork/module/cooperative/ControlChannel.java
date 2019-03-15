package stork.module.cooperative;

import org.globus.ftp.DataChannelAuthentication;
import org.globus.ftp.GridFTPSession;
import org.globus.ftp.Session;
import org.globus.ftp.extended.GridFTPControlChannel;
import org.globus.ftp.extended.GridFTPServerFacade;
import org.globus.ftp.vanilla.*;
import stork.module.CooperativeModule;
import stork.util.StorkUtil;
import stork.module.cooperative.FTPURI;
import java.io.Writer;

public class ControlChannel {
  private static String MODULE_NAME = "Stork GridFTP Module";
  private static String MODULE_VERSION = "0.1";
  public final boolean local, gridftp, hasCred;
  public final FTPServerFacade facade;
  public final FTPControlChannel fc;
  public final BasicClientControlChannel cc;
  public int channelID;
  Writer instantThroughputWriter;


  public ControlChannel(FTPURI u) throws Exception {
    if (u.file) {
      throw new Error("making remote connection to invalid URL");
    }
    local = false;
    facade = null;
    gridftp = u.gridftp;
    hasCred = u.cred != null;
    if (u.gridftp) {
      GridFTPControlChannel gc;
      cc = fc = gc = new GridFTPControlChannel(u.host, u.port);
      gc.open();
      if (u.cred != null) {
        try {
          gc.authenticate(u.cred);
        } catch (Exception e) {
          System.out.println("Error in connecting host " + u.host);
          e.printStackTrace();
          System.exit(-1);
        }
      } else {
        String user = (u.user == null) ? "anonymous" : u.user;
        String pass = (u.pass == null) ? "" : u.pass;
        Reply r = exchange("USER", user);
        if (Reply.isPositiveIntermediate(r)) {
          try {
            execute("PASS", u.pass);
          } catch (Exception e) {
            throw new Exception("bad password");
          }
        } else if (!Reply.isPositiveCompletion(r)) {
          throw new Exception("bad username");
        }
      }
      exchange("SITE CLIENTINFO appname=" + MODULE_NAME +
          ";appver=" +MODULE_VERSION + ";schema=gsiftp;");

      //server.run();
    } else {
      String user = (u.user == null) ? "anonymous" : u.user;
      cc = fc = new FTPControlChannel(u.host, u.port);
      fc.open();

      Reply r = exchange("USER", user);
      if (Reply.isPositiveIntermediate(r)) {
        try {
          execute("PASS", u.pass);
        } catch (Exception e) {
          throw new Exception("bad password");
        }
      } else if (!Reply.isPositiveCompletion(r)) {
        throw new Exception("bad username");
      }
    }
  }

  // Make a local control channel connection to a remote control channel.
  public ControlChannel(ControlChannel rc) throws Exception {
    if (rc.local) {
      throw new Error("making local facade for local channel");
    }
    local = true;
    gridftp = rc.gridftp;
    hasCred = rc.hasCred;
    if (gridftp) {
      facade = new GridFTPServerFacade((GridFTPControlChannel) rc.fc);

      if (!hasCred) {
        ((GridFTPServerFacade) facade).setDataChannelAuthentication(DataChannelAuthentication.NONE);
      }
    } else {
      facade = new FTPServerFacade(rc.fc);
    }
    cc = facade.getControlChannel();
    fc = null;
  }

  // Dumb thing to convert mode/type chars into JGlobus mode ints...
  private static int modeIntValue(char m) throws Exception {
    switch (m) {
      case 'E':
        return GridFTPSession.MODE_EBLOCK;
      case 'B':
        return GridFTPSession.MODE_BLOCK;
      case 'S':
        return GridFTPSession.MODE_STREAM;
      default:
        throw new Error("bad mode: " + m);
    }
  }

  private static int typeIntValue(char t) throws Exception {
    switch (t) {
      case 'A':
        return Session.TYPE_ASCII;
      case 'I':
        return Session.TYPE_IMAGE;
      default:
        throw new Error("bad type: " + t);
    }
  }

  // Change the mode of this channel.
  public void mode(char m) throws Exception {
    if (local) {
      facade.setTransferMode(modeIntValue(m));
    } else {
      execute("MODE", m);
    }
  }

  // Change the data type of this channel.
  public void type(char t) throws Exception {
    if (local) {
      facade.setTransferType(typeIntValue(t));
    } else {
      execute("TYPE", t);
    }
  }

  // Pipe a command whose reply will be read later.
  public void write(Object... args) throws Exception {
    if (local) {
      return;
    }
    fc.write(new Command(StorkUtil.join(args)));
  }

  // Read the reply of a piped command.
  public Reply read() {
    Reply r = null;
    try {
      r = cc.read();
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return r;
  }

  // Execute command, but don't throw on negative reply.
  public Reply exchange(Object... args) throws Exception {
    if (local) {
      return null;
    }
    return fc.exchange(new Command(StorkUtil.join(args)));
  }

  // Execute command, but DO throw on negative reply.
  public Reply execute(Object... args) throws Exception {
    if (local) {
      return null;
    }
    try {
      return fc.execute(new Command(StorkUtil.join(args)));
    } catch (Exception e) {
      // TODO: handle exception
      e.printStackTrace();
      return null;
    }
  }

  // Close the control channels in the chain.
  public void close() throws Exception {
    if (local) {
      facade.close();
    } else {
      write("QUIT");
    }
  }

  public void abort() throws Exception {
    if (local) {
      facade.abort();
    } else {
      write("ABOR");
    }
  }
}
