/**
  * Datanode layout offline upgrade tool
  *
  * upgrade from layout version -56 to -57
  *
  *  kihwal@yahoo.com
  *
 */

package net.fayoly.hadoop;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs.BlockReportReplica;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.StorageInfo;

public class DatanodeUpgrader {
  static int OLD_LAYOUT_VER = -56;
  static int NEW_LAYOUT_VER = -57;

  FileSystem fs = FileSystems.getDefault();
  Properties props = null;

  /* the id to dir mapping for the old layout 56. 256x256 */
  public static File idToBlockDir56(File root, long blockId) {
    int d1 = (int)((blockId >> 16) & 0xff);
    int d2 = (int)((blockId >> 8) & 0xff);
    String path = "subdir" + d1 + "/subdir" + d2;
    return new File(root, path);
  }

  /* the new mapping 32x32 */
  public static File idToBlockDir57(File root, long blockId) {
    int d1 = (int) ((blockId >> 16) & 0x1F);
    int d2 = (int) ((blockId >> 8) & 0x1F);
    String path = "subdir" + d1 + "/subdir" + d2;
    return new File(root, path);
  }

  /**
   * it expects the path to a block pool directory.
   * e.g. /storage/0/hadoop/var/hdfs/data/current/BP-1586417773-9.13.15.56-1363377856192
   */
  public void processReplicaFile(String BPSliceDir) throws IOException {
    File currentDir =  new File(BPSliceDir, "current");
    File previousDir = new File(BPSliceDir, "previous");
    File prevTmpDir = new File(BPSliceDir, "previous.tmp");

    System.out.println("Upgrading " + BPSliceDir);

    // sanity check
    if (!currentDir.exists() || previousDir.exists() || prevTmpDir.exists()) {
      System.err.println("Previous upgrade attempt detected. Aborting...");
      System.err.println("Normal DN startup will fix it through the slow path.");
      throw new IOException("Previous upgrade attempt detected. Aborting...");
    }

    // load and check the version of the current directory.
    loadAndCheckVersion(currentDir);

    // Cache file path
    File replicaFile = new File(currentDir, "replicas");

    // Check whether the file exists or not.
    if (!replicaFile.exists()) {
      System.err.println("Replica Cache file: "+  replicaFile.getPath() +
		" does not exist. Upgrade using normal DN startup. ");
      throw new IOException("No cache file found. Upgrade using normal DN startup.");
    }

    // remove trash directory
    File trash = new File(BPSliceDir, "trash");
    FileUtil.fullyDelete(trash);

    FileInputStream inputStream = null;
    HashSet<Block> finalizedBlocks = new HashSet<Block>();
    HashSet<Block> ucBlocks = new HashSet<Block>();
    inputStream = new FileInputStream(replicaFile);
    BlockListAsLongs blocksList =  BlockListAsLongs.readFrom(inputStream);
    Iterator<BlockReportReplica> iterator = blocksList.iterator();
    while (iterator.hasNext()) {
      BlockReportReplica replica = iterator.next();
      switch (replica.getState()) {
      case FINALIZED:
        finalizedBlocks.add(new Block(replica));
        break;
      case RBW:
        ucBlocks.add(new Block(replica));
        break;
      default:
        // ignore all other temporary blocks
      }
    }
    inputStream.close();

    // It would have blown up if the input was truncated or malformed.
    // Safe to proceed.

    // To take advantage of hadoop storage upgrade failure handling, we do the
    // following:
    //  rename current to previous.tmp
    //  create new current.
    //  start creating links in finalized and rbw directory.
    //  write VERSION file with the new version.
    // If something fails in the middle, regular hadoop startup/upgrade will
    // be able to rewind and retry.

    // rename current to previous.tmp
    //NativeIO.renameTo(currentDir, prevTmpDir);
    if (!currentDir.renameTo(prevTmpDir)) {
      throw new IOException("Rename from current to previous.tmp failed.");
    }

    // create hard links in finalized dir
    File sourceBaseDir = new File(prevTmpDir, "finalized");
    File destBaseDir = new File(currentDir, "finalized");
    destBaseDir.mkdirs();
    for (Block r : finalizedBlocks) {
      long blockId = r.getBlockId();
      File srcDir = idToBlockDir56(sourceBaseDir, blockId);
      File dstDir = idToBlockDir57(destBaseDir, blockId);
      if (!dstDir.exists()) {
        dstDir.mkdirs();
      }
      createBlockLinks(r, srcDir, dstDir);
    }
 
    // rbw blocks.
    sourceBaseDir = new File(prevTmpDir, "rbw");
    destBaseDir = new File(currentDir, "rbw");
    destBaseDir.mkdirs();
    for (Block r : ucBlocks) {
      createBlockLinks(r, sourceBaseDir, destBaseDir);
    }

    // write the version file
    updateVersionFile(currentDir);
    // remove the scanner cursor file based on the old layout
    File scanner = new File(BPSliceDir, "scanner.cursor");
    scanner.delete();

    // rename previous.tmp to previous. 
    prevTmpDir.renameTo(previousDir);
  }

  // Create hard links
  void createBlockLinks(Block r, File srcDir, File dstDir) throws IOException {
    long blockId = r.getBlockId();
    long genStamp = r.getGenerationStamp();
    String blkName = r.getBlockName();
    String metaSfx = "_" + genStamp + Block.METADATA_EXTENSION;

    // block file
    Path srcPath = fs.getPath(srcDir.getPath(), blkName);
    Path dstPath = fs.getPath(dstDir.getPath(), blkName);
    if (!Files.exists(dstPath)) {
      Files.createLink(dstPath, srcPath);
    } else {
      System.out.println(blockId + " already exists. Skipping.");
      return;
    }

    // meta file
    srcPath = fs.getPath(srcDir.getPath(), blkName + metaSfx);
    dstPath = fs.getPath(dstDir.getPath(), blkName + metaSfx);
    Files.createLink(dstPath, srcPath);
  }

  // Update the version file with the correct layout version
  void updateVersionFile(File newDir) throws IOException {
    File newVer = new File(newDir, "VERSION");

    if (props == null) {
      throw new IOException("BUG: Old VERSION file not loaded.");
    }
    props.setProperty("layoutVersion", String.valueOf(NEW_LAYOUT_VER));
    Storage.writeProperties(newVer, props);
  
  }

  // Load and check the current version
  void loadAndCheckVersion(File dir) throws IOException {
    File verFile = new File(dir, "VERSION");
    props = StorageInfo.readPropertiesFile(verFile);

    // Check existing version.
    String ver = props.getProperty("layoutVersion");
    if (!ver.equals(String.valueOf(OLD_LAYOUT_VER))) {
       System.err.println("Unexpected version: " + ver);
       throw new IOException("Unexpected version: " + ver);
    }
  }

  static HashSet<String> getBPSliceDirs() throws Exception {
    File mounts = new File("/proc/mounts");
    BufferedReader br = new BufferedReader(new FileReader(mounts));
    String line = null;
    String bpName = null;
    HashSet<String> bpDirs = new HashSet<String>();

    while ((line = br.readLine()) != null) {
      String[] mntInfo = line.split("\\s+");
      if (mntInfo[1].startsWith("/grid/") && mntInfo[3].contains("rw")) {
        if (bpName == null) {
          File dataDir = new File(mntInfo[1], "hadoop/var/hdfs/data/current");
          if (!dataDir.exists()) {
            // unformatted data dir
            continue;
          }
          String[] children = dataDir.list();
          for (int i = 0; i < children.length; i++) {
            if (children[i].startsWith("BP-")) {
              // no federation. we break out on the first BP dir.
              bpName = children[i];
              break;
            }
          }
          // no bp dir. may be unformatted.
          if (bpName == null) {
            continue;
          }
        }
        // at this point bpName must be set. Add to the set.
        bpDirs.add(mntInfo[1] + "/hadoop/var/hdfs/data/current/" + bpName);
      }
    }
    br.close();
    return bpDirs;
  }

  public static void main(String args[]) throws Exception {
    final List<IOException> exceptions = Collections.synchronizedList(
        new ArrayList<IOException>());
    List<Thread> upgradeThreads = new ArrayList<Thread>();
    for (final String dir : DatanodeUpgrader.getBPSliceDirs()) {
      Thread t = new Thread() {
        public void run() {
          try {
            DatanodeUpgrader dnUp = new DatanodeUpgrader();
            dnUp.processReplicaFile(dir);
          } catch (IOException ioe) {
            System.err.println(ioe);
            exceptions.add(ioe);
          }
        }
      };
      upgradeThreads.add(t);
      t.start();
    }
    for (Thread t : upgradeThreads) {
      try {
        t.join();
      } catch (InterruptedException ie) {
        throw new IOException(ie);
      }
    }
    if (!exceptions.isEmpty()) {
      throw exceptions.get(0);
    }
    System.out.println("Upgrade complete.");
  }
}
