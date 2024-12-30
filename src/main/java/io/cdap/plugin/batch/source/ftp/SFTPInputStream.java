/*
 * Copyright © 2017 Cask Data, Inc.
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

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import io.cdap.cdap.api.exception.ErrorCategory;
import io.cdap.cdap.api.exception.ErrorType;
import io.cdap.cdap.api.exception.ErrorUtils;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.util.StringUtils;
import java.io.IOException;
import java.io.InputStream;

/**
 * {@link SFTPInputStream}, copied from Hadoop and modified, that doesn't throw an exception when seeks are attempted
 * to the current position. Position equality check logic in {@link SFTPInputStream#seek} is the only change from the
 * original class in Hadoop. This change is required since {@link LineRecordReader} calls {@link SFTPInputStream#seek}
 * with value of 0. TODO: This file can be removed once https://cdap.atlassian.net/browse/CDAP-5387 is addressed.
 */
class SFTPInputStream extends FSInputStream {

  public static final String E_SEEK_NOTSUPPORTED = "Seek not supported";
  public static final String E_CLIENT_NULL =
    "SFTP client null or not connected";
  public static final String E_NULL_INPUTSTREAM = "Null InputStream";
  public static final String E_STREAM_CLOSED = "Stream closed";
  public static final String E_CLIENT_NOTCONNECTED = "Client not connected";

  private InputStream wrappedStream;
  private ChannelSftp channel;
  private FileSystem.Statistics stats;
  private boolean closed;
  private long pos;

  SFTPInputStream(InputStream stream, ChannelSftp channel,
                  FileSystem.Statistics stats) {

    if (stream == null) {
      throw new IllegalArgumentException(E_NULL_INPUTSTREAM);
    }
    if (channel == null || !channel.isConnected()) {
      throw new IllegalArgumentException(E_CLIENT_NULL);
    }
    this.wrappedStream = stream;
    this.channel = channel;
    this.stats = stats;

    this.pos = 0;
    this.closed = false;
  }

  // We don't support seek unless the current position is same as the desired position.
  @Override
  public void seek(long position) {
    // If seek is to the current pos, then simply return. This logic was added so that the seek call in
    // LineRecordReader#initialize method to '0' does not fail.
    try {
      if (getPos() == position) {
        return;
      }
    } catch (IOException e) {
      String errorMessage = String.format(E_SEEK_NOTSUPPORTED + "Failure reason is %s.", e.getMessage());
      throw ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN),
        E_SEEK_NOTSUPPORTED, errorMessage, ErrorType.SYSTEM, true, new IOException(E_SEEK_NOTSUPPORTED));
    }
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    throw new IOException(E_SEEK_NOTSUPPORTED);
  }

  @Override
  public long getPos() throws IOException {
    return pos;
  }

  @Override
  public synchronized int read() {
    if (closed) {
      String errorMessage = "Unable to read, stream closed unexpectedly.";
      throw ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN),
        E_STREAM_CLOSED, errorMessage, ErrorType.SYSTEM, true, new IOException(E_STREAM_CLOSED));
    }

    try {
      int byteRead = wrappedStream.read();
      if (byteRead >= 0) {
        pos++;
      }
      if (stats != null & byteRead >= 0) {
        stats.incrementBytesRead(1);
      }
      return byteRead;
    } catch (IOException e) {
      String errorReason = "Unable to read wrapped input stream.";
      String errorMessage = String.format("Failed to read wrapped input stream with reason %s ", e.getMessage());
      throw ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN),
        errorReason, errorMessage, ErrorType.SYSTEM, true, e);
    }
  }

  @Override
  public synchronized int read(byte[] buf, int off, int len) {
    if (closed) {
      String errorReason = E_STREAM_CLOSED + " Unable to read.";
      String errorMessage = String.format("Stream closed. Failed to read buffer stream of length %s and offset %s",
        len, off);
      throw ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN),
        errorReason, errorMessage, ErrorType.SYSTEM, true, new IOException(E_STREAM_CLOSED));
    }

    int result = 0;
    try {
      result = wrappedStream.read(buf, off, len);
      if (result > 0) {
        pos += result;
      }
      if (stats != null & result > 0) {
        stats.incrementBytesRead(result);
      }

      return result;
    } catch (IOException e) {
      String errorReason = "Unable to read wrapped input stream.";
      String errorMessage = String.format("Failed to read wrapped input stream of length %s and offset %s " +
        "with reason: %s", len, off, e.getMessage());
      throw ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN),
        errorReason, errorMessage, ErrorType.SYSTEM, true, e);
    }
  }

  @Override
  public synchronized void close() {
    try {
      if (closed) {
        return;
      }
      super.close();
      closed = true;
      if (!channel.isConnected()) {
        throw new IOException(E_CLIENT_NOTCONNECTED);
      }

      Session session = channel.getSession();
      channel.disconnect();
      session.disconnect();
    } catch (JSchException e) {
      String errorReason = "Error occurred while retrieving session.";
      String errorMessage = String.format("Failed to retrieving session with reason: %s", e.getMessage());
      throw ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN),
        errorReason, errorMessage, ErrorType.SYSTEM, true, new IOException(StringUtils.stringifyException(e)));
    } catch (IOException e) {
      String errorReason = "Error occurred while closing stream.";
      String errorMessage = String.format("Failed to close stream with reason: %s", e.getMessage());
      throw ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN),
        errorReason, errorMessage, ErrorType.SYSTEM, true, e);
    }
  }
}
