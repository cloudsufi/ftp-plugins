/*
 * Copyright © 2023 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.cdap.plugin.batch.source.ftp;

import io.cdap.cdap.api.exception.ErrorCategory;
import io.cdap.cdap.api.exception.ErrorType;
import io.cdap.cdap.api.exception.ErrorUtils;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.ftp.FTPException;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

/**
 * FTP FileSystem.
 * This class and its dependency classes were copied from hadoop-2.3 to support FTP FileSystem.
 * There are issues with existing FTPFileSystem class from hadoop for having special characters like colon
 * and ampersand in the username and password. Patching this class to fix that. initialize method is changed to fix
 * that, now username and password will not be fetched from URI but from configuration object to avoid issues if
 * username or password are having any special characters.
 */
public class FTPFileSystem extends FileSystem {

  public static final Logger LOG = LoggerFactory.getLogger(FTPFileSystem.class);

  public static final int DEFAULT_BUFFER_SIZE = 1024 * 1024;

  public static final int DEFAULT_BLOCK_SIZE = 4 * 1024;

  public static final Integer DEFAULT_CONNECTION_TIMEOUT_MS = 30000;

  public static final String FS_CONNECT_TIMEOUT = "fs.connect.timeout";

  private URI uri;

  /**
   * Return the protocol scheme for the FileSystem.
   * <p/>
   *
   * @return <code>ftp</code>
   */
  @Override
  public String getScheme() {
    return "ftp";
  }

  @Override
  public void initialize(URI uri, Configuration conf) throws IOException { // get
    super.initialize(uri, conf);
    // get host information from uri (overrides info in conf)
    String host = uri.getHost();
    host = (host == null) ? conf.get("fs.ftp.host", null) : host;
    if (host == null) {
      String errorReason = "Invalid host specified";
      String errorMessage = "Unable to initialize file system. Invalid host specified, host not found.";
      throw ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN),
        errorReason, errorMessage, ErrorType.SYSTEM, true, new IOException(errorReason));
    }
    conf.set("fs.ftp.host", host);

    // get port information from uri, (overrides info in conf)
    int port = uri.getPort();
    port = (port == -1) ? FTP.DEFAULT_PORT : port;
    conf.setInt("fs.ftp.host.port", port);

    String user = conf.get("fs.ftp.user." + host, null);
    String password = conf.get("fs.ftp.password." + host, null);
    if (user == null) {
      String errorReason = "No user specified";
      String errorMessage = "Unable to initialize file system. Invalid user specified, user not found.";
      throw ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN),
        errorReason, errorMessage, ErrorType.SYSTEM, true, new IOException(errorReason));
    }

    conf.set("fs.ftp.user." + host, user);
    if (password != null) {
      conf.set("fs.ftp.password." + host, password);
    } else {
      conf.set("fs.ftp.password." + host, null);
    }
    setConf(conf);
    this.uri = uri;
  }

  /**
   * Connect to the FTP server using configuration parameters *
   *
   * @return An FTPClient instance
   * @throws IOException
   */
  private FTPClient connect() {
    FTPClient client = null;
    Configuration conf = getConf();
    String host = conf.get("fs.ftp.host");
    int port = conf.getInt("fs.ftp.host.port", FTP.DEFAULT_PORT);
    String user = conf.get("fs.ftp.user." + host);
    String password = conf.get("fs.ftp.password." + host);
    int connectTimeout = conf.getInt(FS_CONNECT_TIMEOUT, DEFAULT_CONNECTION_TIMEOUT_MS);
    client = new FTPClient();
    client.setConnectTimeout(connectTimeout);
    try {
      client.connect(host, port);
      int reply = client.getReplyCode();
      if (!FTPReply.isPositiveCompletion(reply)) {
        throw new IOException(String.format("Server:%s, refused connection on port: %s.", host, port));
      } else {
        if (client.login(user, password)) {
          client.setFileTransferMode(FTP.BLOCK_TRANSFER_MODE);
          client.setFileType(FTP.BINARY_FILE_TYPE);
          client.setBufferSize(DEFAULT_BUFFER_SIZE);
        } else {
          throw new IOException(String.format("Login failed on server:%s, port: %s.", host, port));
        }
      }
    } catch (IOException e) {
      String errorReason = String.format("Error connecting with client on server: %s, on port: %s.", host, port);
      String errorMessage = String.format("Error connecting with client on server: %s, on port: %s. " +
        "Failure reason is %s.", host, port, e.getMessage());
      throw ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN),
        errorReason, errorMessage, ErrorType.SYSTEM, true, e);
    }
    return client;
  }

  /**
   * Logout and disconnect the given FTPClient. *
   *
   * @param client
   * @throws IOException
   */
  private void disconnect(FTPClient client) {
    if (client != null) {
      if (!client.isConnected()) {
        throw new FTPException("Client not connected");
      }
      boolean logoutSuccess;
      try {
        logoutSuccess = client.logout();
        client.disconnect();
        if (!logoutSuccess) {
          LOG.warn("Logout failed while disconnecting, error code: {}", client.getReplyCode());
        }
      } catch (IOException e) {
        String errorReason = "Unable to log-out the client";
        String errorMessage = String.format("Unable to  log-out the client. Failure reason is %s.", e.getMessage());
        throw ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN),
          errorReason, errorMessage, ErrorType.SYSTEM, true, e);
      }
    }
  }

  /**
   * Resolve against given working directory. *
   *
   * @param workDir
   * @param path
   * @return
   */
  private Path makeAbsolute(Path workDir, Path path) {
    if (path.isAbsolute()) {
      return path;
    }
    return new Path(workDir, path);
  }

  @Override
  public FSDataInputStream open(Path file, int bufferSize) {
    FTPClient client = connect();
    Path workDir;
    try {
      workDir = new Path(client.printWorkingDirectory());
      Path absolute = makeAbsolute(workDir, file);
      FileStatus fileStat = getFileStatus(client, absolute);
      if (fileStat.isDirectory()) {
        disconnect(client);
        throw new IOException("Path " + file + " is a directory.");
      }
      client.allocate(bufferSize);
      Path parent = absolute.getParent();
      // Change to parent directory on the
      // server. Only then can we read the
      // file
      // on the server by opening up an InputStream. As a side effect the working
      // directory on the server is changed to the parent directory of the file.
      // The FTP client connection is closed when close() is called on the
      // FSDataInputStream.
      client.changeWorkingDirectory(parent.toUri().getPath());
      InputStream is = client.retrieveFileStream(file.getName());
      FSDataInputStream fis = new FSDataInputStream(new FTPInputStream(is,
        client, statistics));
      if (!FTPReply.isPositivePreliminary(client.getReplyCode())) {
        // The ftpClient is an inconsistent state. Must close the stream
        // which in turn will logout and disconnect from FTP server
        fis.close();
        throw new IOException("Unable to open file: " + file + ", Aborting");
      }
      return fis;
    } catch (IOException e) {
      String errorReason = String.format("Unable to open file %s, aborting process.", file);
      String errorMessage = String.format("Error opening file %s, aborting process. Failure reason is %s.",
        file, e.getMessage());
      throw ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN),
        errorReason, errorMessage, ErrorType.SYSTEM, true, e);
    }
  }

  /**
   * A stream obtained via this call must be closed before using other APIs of
   * this class or else the invocation will block.
   */
  @Override
  public FSDataOutputStream create(Path file, FsPermission permission,
                                   boolean overwrite, int bufferSize, short replication, long blockSize,
                                   Progressable progress) {
    final FTPClient client = connect();
    Path workDir;
    try {
      workDir = new Path(client.printWorkingDirectory());
      Path absolute = makeAbsolute(workDir, file);
      if (exists(client, file)) {
        if (overwrite) {
          delete(client, file);
        } else {
          disconnect(client);
          throw new IOException("File already exists: " + file);
        }
      }

      Path parent = absolute.getParent();
      if (parent == null || !mkdirs(client, parent, FsPermission.getDirDefault())) {
        parent = (parent == null) ? new Path("/") : parent;
        disconnect(client);
        throw new IOException("create(): Mkdirs failed to create: " + parent);
      }
      client.allocate(bufferSize);
      // Change to parent directory on the server. Only then can we write to the
      // file on the server by opening up an OutputStream. As a side effect the
      // working directory on the server is changed to the parent directory of the
      // file. The FTP client connection is closed when close() is called on the
      // FSDataOutputStream.
      client.changeWorkingDirectory(parent.toUri().getPath());
      FSDataOutputStream fos = new FSDataOutputStream(client.storeFileStream(file
        .getName()), statistics) {
        @Override
        public void close() throws IOException {
          super.close();
          if (!client.isConnected()) {
            throw new FTPException("Client not connected");
          }
          boolean cmdCompleted = client.completePendingCommand();
          disconnect(client);
          if (!cmdCompleted) {
            throw new FTPException("Could not complete transfer, Reply Code - "
              + client.getReplyCode());
          }
        }
      };
      if (!FTPReply.isPositivePreliminary(client.getReplyCode())) {
        // The ftpClient is an inconsistent state. Must close the stream
        // which in turn will logout and disconnect from FTP server
        fos.close();
        throw new IOException("Unable to create file: " + file + ", Aborting");
      }
      return fos;
    } catch (IOException e) {
      String errorReason = String.format("Unable to create file %s, aborting process.", file);
      String errorMessage = String.format("Error creating file %s, aborting process. Failure reason is %s.",
        file, e.getMessage());
      throw ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN),
        errorReason, errorMessage, ErrorType.SYSTEM, true, e);
    }
  }

  /**
   * This optional operation is not yet supported.
   */
  @Override
  public FSDataOutputStream append(Path f, int bufferSize,
                                   Progressable progress) throws IOException {
    throw new IOException("Not supported");
  }

  /**
   * Convenience method, so that we don't open a new connection when using this
   * method from within another method. Otherwise every API invocation incurs
   * the overhead of opening/closing a TCP connection.
   */
  private boolean exists(FTPClient client, Path file) {
    return getFileStatus(client, file) != null;
  }

  @Override
  public boolean delete(Path file, boolean recursive) throws IOException {
    FTPClient client = connect();
    try {
      return delete(client, file, recursive);
    } finally {
      disconnect(client);
    }
  }

  /**
   * @deprecated Use delete(Path, boolean) instead
   */
  @Deprecated
  private boolean delete(FTPClient client, Path file) {
    return delete(client, file, false);
  }

  /**
   * Convenience method, so that we don't open a new connection when using this
   * method from within another method. Otherwise every API invocation incurs
   * the overhead of opening/closing a TCP connection.
   */
  private boolean delete(FTPClient client, Path file, boolean recursive) {
    Path workDir;
    try {
      workDir = new Path(client.printWorkingDirectory());
      Path absolute = makeAbsolute(workDir, file);
      String pathName = absolute.toUri().getPath();
      FileStatus fileStat = getFileStatus(client, absolute);
      if (fileStat.isFile()) {
        return client.deleteFile(pathName);
      }
      FileStatus[] dirEntries = listStatus(client, absolute);
      if (dirEntries.length > 0 && !recursive) {
        throw new IOException("Directory: " + file + " is not empty.");
      }
      if (dirEntries != null) {
        for (FileStatus dirEntry : dirEntries) {
          delete(client, new Path(absolute, dirEntry.getPath()), recursive);
        }
      }
      return client.removeDirectory(pathName);
    } catch (IOException e) {
      String errorReason = String.format("Unable to delete file %s, aborting process.", file);
      String errorMessage = String.format("Error deleting file %s, aborting process. " +
        "Failure reason is %s.", file, e.getMessage());
      throw ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN),
        errorReason, errorMessage, ErrorType.SYSTEM, true, e);
    }
  }

  private FsAction getFsAction(int accessGroup, FTPFile ftpFile) {
    FsAction action = FsAction.NONE;
    if (ftpFile.hasPermission(accessGroup, FTPFile.READ_PERMISSION)) {
      action.or(FsAction.READ);
    }
    if (ftpFile.hasPermission(accessGroup, FTPFile.WRITE_PERMISSION)) {
      action.or(FsAction.WRITE);
    }
    if (ftpFile.hasPermission(accessGroup, FTPFile.EXECUTE_PERMISSION)) {
      action.or(FsAction.EXECUTE);
    }
    return action;
  }

  private FsPermission getPermissions(FTPFile ftpFile) {
    FsAction user, group, others;
    user = getFsAction(FTPFile.USER_ACCESS, ftpFile);
    group = getFsAction(FTPFile.GROUP_ACCESS, ftpFile);
    others = getFsAction(FTPFile.WORLD_ACCESS, ftpFile);
    return new FsPermission(user, group, others);
  }

  @Override
  public URI getUri() {
    return uri;
  }

  @Override
  public FileStatus[] listStatus(Path file) {
    FTPClient client = connect();
    try {
      return listStatus(client, file);
    } finally {
      disconnect(client);
    }
  }

  /**
   * Convenience method, so that we don't open a new connection when using this
   * method from within another method. Otherwise every API invocation incurs
   * the overhead of opening/closing a TCP connection.
   */
  private FileStatus[] listStatus(FTPClient client, Path file) {
    Path workDir = null;
    try {
      workDir = new Path(client.printWorkingDirectory());
      Path absolute = makeAbsolute(workDir, file);
      FileStatus fileStat = getFileStatus(client, absolute);
      if (fileStat.isFile()) {
        return new FileStatus[]{fileStat};
      }
      FTPFile[] ftpFiles = client.listFiles(absolute.toUri().getPath());
      FileStatus[] fileStats = new FileStatus[ftpFiles.length];
      for (int i = 0; i < ftpFiles.length; i++) {
        fileStats[i] = getFileStatus(ftpFiles[i], absolute);
      }
      return fileStats;
    } catch (IOException e) {
      String errorReason = String.format("Unable to fetch and list the status for file %s, aborting process.", file);
      String errorMessage = String.format("Error fetching and listing status for file %s, aborting process. " +
        "Failure reason is %s.", file, e.getMessage());
      throw ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN),
        errorReason, errorMessage, ErrorType.SYSTEM, true, e);
    }
  }

  @Override
  public FileStatus getFileStatus(Path file) throws IOException {
    FTPClient client = connect();
    try {
      return getFileStatus(client, file);
    } finally {
      disconnect(client);
    }
  }

  /**
   * Convenience method, so that we don't open a new connection when using this
   * method from within another method. Otherwise every API invocation incurs
   * the overhead of opening/closing a TCP connection.
   */
  private FileStatus getFileStatus(FTPClient client, Path file) {
    FileStatus fileStat = null;
    Path workDir = null;
    try {
      workDir = new Path(client.printWorkingDirectory());
      Path absolute = makeAbsolute(workDir, file);
      Path parentPath = absolute.getParent();
      if (parentPath == null) { // root dir
        long length = -1; // Length of root dir on server not known
        boolean isDir = true;
        int blockReplication = 1;
        long blockSize = DEFAULT_BLOCK_SIZE; // Block Size not known.
        long modTime = -1; // Modification time of root dir not known.
        Path root = new Path("/");
        return new FileStatus(length, isDir, blockReplication, blockSize,
          modTime, root.makeQualified(this));
      }
      String pathName = parentPath.toUri().getPath();
      FTPFile[] ftpFiles = client.listFiles(pathName);
      if (ftpFiles != null) {
        for (FTPFile ftpFile : ftpFiles) {
          if (ftpFile.getName().equals(file.getName())) { // file found in dir
            fileStat = getFileStatus(ftpFile, parentPath);
            break;
          }
        }
        if (fileStat == null) {
          throw new FileNotFoundException("File " + file + " does not exist.");
        }
      } else {
        throw new FileNotFoundException("File " + file + " does not exist.");
      }
      return fileStat;
    } catch (IOException e) {
      String errorReason = String.format("Unable to fetch the status for file %s, aborting process.", file);
      String errorMessage = String.format("Error fetching status for file %s, aborting process. " +
        "Failure reason is %s.", file, e.getMessage());
      throw ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN),
        errorReason, errorMessage, ErrorType.SYSTEM, true, e);
    }
  }

  /**
   * Convert the file information in FTPFile to a {@link FileStatus} object. *
   *
   * @param ftpFile
   * @param parentPath
   * @return FileStatus
   */
  private FileStatus getFileStatus(FTPFile ftpFile, Path parentPath) {
    long length = ftpFile.getSize();
    boolean isDir = ftpFile.isDirectory();
    int blockReplication = 1;
    // Using default block size since there is no way in FTP client to know of
    // block sizes on server. The assumption could be less than ideal.
    long blockSize = DEFAULT_BLOCK_SIZE;
    long modTime = ftpFile.getTimestamp().getTimeInMillis();
    long accessTime = 0;
    FsPermission permission = getPermissions(ftpFile);
    String user = ftpFile.getUser();
    String group = ftpFile.getGroup();
    Path filePath = new Path(parentPath, ftpFile.getName());
    return new FileStatus(length, isDir, blockReplication, blockSize, modTime,
                          accessTime, permission, user, group, filePath.makeQualified(this));
  }

  @Override
  public boolean mkdirs(Path file, FsPermission permission) {
    FTPClient client = connect();
    try {
      return mkdirs(client, file, permission);
    } finally {
      disconnect(client);
    }
  }

  /**
   * Convenience method, so that we don't open a new connection when using this
   * method from within another method. Otherwise every API invocation incurs
   * the overhead of opening/closing a TCP connection.
   */
  private boolean mkdirs(FTPClient client, Path file, FsPermission permission) {
    boolean created = true;
    Path workDir;
    try {
      workDir = new Path(client.printWorkingDirectory());
      Path absolute = makeAbsolute(workDir, file);
      String pathName = absolute.getName();
      if (!exists(client, absolute)) {
        Path parent = absolute.getParent();
        created = (parent == null || mkdirs(client, parent, FsPermission
          .getDirDefault()));
        if (created) {
          String parentDir = parent.toUri().getPath();
          client.changeWorkingDirectory(parentDir);
          created = created && client.makeDirectory(pathName);
        }
      } else if (isFile(client, absolute)) {
        throw new IOException(String.format("Can't make directory for path %s since it is a file.", absolute));
      }
      return created;
    } catch (IOException e) {
      String errorReason = String.format("Unable to make directory for file %s, aborting process.", file);
      String errorMessage = String.format("Error making directory for file %s, aborting process. " +
        "Failure reason is %s.", file, e.getMessage());
      throw ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN),
        errorReason, errorMessage, ErrorType.SYSTEM, true, e);
    }
  }

  /**
   * Convenience method, so that we don't open a new connection when using this
   * method from within another method. Otherwise every API invocation incurs
   * the overhead of opening/closing a TCP connection.
   */
  private boolean isFile(FTPClient client, Path file) {
    return getFileStatus(client, file).isFile();
  }

  /*
   * Assuming that parent of both source and destination is the same. Is the
   * assumption correct or it is suppose to work like 'move' ?
   */
  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    FTPClient client = connect();
    try {
      return rename(client, src, dst);
    } finally {
      disconnect(client);
    }
  }

  /**
   * Convenience method, so that we don't open a new connection when using this
   * method from within another method. Otherwise every API invocation incurs
   * the overhead of opening/closing a TCP connection.
   *
   * @param client
   * @param src
   * @param dst
   * @return
   * @throws IOException
   */
  private boolean rename(FTPClient client, Path src, Path dst) {
    Path workDir;
    try {
      workDir = new Path(client.printWorkingDirectory());
      Path absoluteSrc = makeAbsolute(workDir, src);
      Path absoluteDst = makeAbsolute(workDir, dst);
      if (!exists(client, absoluteSrc)) {
        throw new IOException("Source path " + src + " does not exist");
      }
      if (exists(client, absoluteDst)) {
        throw new IOException("Destination path " + dst
          + " already exist, cannot rename!");
      }
      String parentSrc = absoluteSrc.getParent().toUri().toString();
      String parentDst = absoluteDst.getParent().toUri().toString();
      String from = src.getName();
      String to = dst.getName();
      if (!parentSrc.equals(parentDst)) {
        throw new IOException("Cannot rename parent(source): " + parentSrc
          + ", parent(destination):  " + parentDst);
      }
      client.changeWorkingDirectory(parentSrc);
      return client.rename(from, to);
    } catch (IOException e) {
      String errorReason = String.format("Unable to rename the source path %s as destination path %s already exist, " +
        "aborting process.", src, dst);
      String errorMessage = String.format("Error rename the source path %s as destination path %s already exist, " +
        "aborting process. Failure reason is %s.", src, dst, e.getMessage());
      throw ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN),
        errorReason, errorMessage, ErrorType.SYSTEM, true, e);
    }

  }

  @Override
  public Path getWorkingDirectory() {
    // Return home directory always since we do not maintain state.
    return getHomeDirectory();
  }

  @Override
  public Path getHomeDirectory() {
    FTPClient client = null;
    try {
      client = connect();
      return new Path(client.printWorkingDirectory());
    } catch (IOException ioe) {
      throw new FTPException("Failed to get home directory", ioe);
    } finally {
      disconnect(client);
    }
  }

  @Override
  public void setWorkingDirectory(Path newDir) {
    // we do not maintain the working directory state
  }
}

