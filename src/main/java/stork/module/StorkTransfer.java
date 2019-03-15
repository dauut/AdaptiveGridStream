package stork.module;

public interface StorkTransfer extends Runnable {
  void start();

  void stop();

  int waitFor();
}
