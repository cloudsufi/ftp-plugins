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
import org.apache.commons.net.ftp.FTPClient;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.ftp.FTPException;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;
import java.io.InputStream;

/**
 * {@link FTPInputStream}, copied from Hadoop and modified, that doesn't throw an exception when seeks are attempted
 * to the current position. Position equality check logic in {@link FTPInputStream#seek} is the only change from the
 * original class in Hadoop. This change is required since {@link LineRecordReader} calls {@link FTPInputStream#seek}
 * with value of 0. TODO: This file can be removed once https://cdap.atlassian.net/browse/CDAP-5387 is addressed.
 */
public class FTPInputStream extends FSInputStream {

  public static final String SEEK_NOT_SUPPORTED = "Seek not supported";
  InputStream wrappedStream;
  FTPClient client;
  FileSystem.Statistics stats;
  boolean closed;
  long pos;

  public FTPInputStream(InputStream stream, FTPClient client,
                        FileSystem.Statistics stats) {
    if (stream == null) {
      throw new IllegalArgumentException("Null InputStream");
    }
    if (client == null || !client.isConnected()) {
      throw new IllegalArgumentException("FTP client null or not connected");
    }
    this.wrappedStream = stream;
    this.client = client;
    this.stats = stats;
    this.pos = 0;
    this.closed = false;
  }

  @Override
  public long getPos() throws IOException {
    return pos;
  }

  // We don't support seek unless the current position is same as the desired position.
  @Override
  public void seek(long pos) {
    // If seek is to the current pos, then simply return. This logic was added so that the seek call in
    // LineRecordReader#initialize method to '0' does not fail.
    try {
      if (getPos() == pos) {
        return;
      }
      throw new IOException(SEEK_NOT_SUPPORTED);
    } catch (IOException e) {
      String errorMessage = String.format(SEEK_NOT_SUPPORTED + " Failure reason is %s.", e.getMessage());
      throw ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN),
        SEEK_NOT_SUPPORTED, errorMessage, ErrorType.SYSTEM, true, e);
    }

  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    throw new IOException(SEEK_NOT_SUPPORTED);
  }

  @Override
  public synchronized int read() {
    int byteRead;
    try {
      if (closed) {
        throw new IOException("Stream closed");
      }

      byteRead = wrappedStream.read();
      if (byteRead >= 0) {
        pos++;
      }
      if (stats != null && byteRead >= 0) {
        stats.incrementBytesRead(1);
      }
      return byteRead;
    } catch (IOException e) {
      String errorReason = "Unable to read";
      String errorMessage = String.format("Unable to read input stream, aborting process. " +
        "Failure reason is %s.", e.getMessage());
      throw ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN),
        errorReason, errorMessage, ErrorType.SYSTEM, true, e);
    }
  }

  @Override
  public synchronized int read(byte[] buf, int off, int len) {
    try {
      if (closed) {
        throw new IOException("Stream closed");
      }

      int result = 0;
      result = wrappedStream.read(buf, off, len);
      if (result > 0) {
        pos += result;
      }
      if (stats != null && result > 0) {
        stats.incrementBytesRead(result);
      }

      return result;
    } catch (IOException e) {
      String errorReason = "Unable to read buffer.";
      String errorMessage = String.format("Unable to read input buffer stream, aborting process. " +
        "Failure reason is %s.", e.getMessage());
      throw ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN),
        errorReason, errorMessage, ErrorType.SYSTEM, true, e);
    }
  }

  @Override
  public synchronized void close() {
    try {
      if (closed) {
        throw new IOException("Stream closed");
      }
      super.close();
      closed = true;
      if (!client.isConnected()) {
        throw new FTPException("Client not connected");
      }

      boolean cmdCompleted = client.completePendingCommand();
      client.logout();
      client.disconnect();

      if (!cmdCompleted) {
        throw new FTPException("Could not complete transfer, Reply Code - "
          + client.getReplyCode());
      }
    } catch (IOException e) {
      String errorReason = "Unable to close connection.";
      String errorMessage = String.format("Unable to close connection, aborting process. Failure reason is %s.",
        e.getMessage());
      throw ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN),
        errorReason, errorMessage, ErrorType.SYSTEM, true, e);
    }
  }

  // Not supported.

  @Override
  public boolean markSupported() {
    return false;
  }

  @Override
  public void mark(int readLimit) {
    // Do nothing
  }

  @Override
  public void reset() throws IOException {
    throw new IOException("Mark not supported");
  }
}
