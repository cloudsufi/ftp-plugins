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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.exception.ErrorCategory;
import io.cdap.cdap.api.exception.ErrorType;
import io.cdap.cdap.api.exception.ErrorUtils;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.common.batch.JobUtils;
import io.cdap.plugin.format.FileFormat;
import io.cdap.plugin.format.plugin.FileSourceProperties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import javax.ws.rs.core.UriBuilder;

/**
 * Config class for the source
 */
@SuppressWarnings({"FieldCanBeLocal", "unused"})
public class FTPConfig extends PluginConfig implements FileSourceProperties {
  public static final Logger LOG = LoggerFactory.getLogger(FTPConfig.class);
  private static final Gson GSON = new Gson();
  @SuppressWarnings("UnstableApiUsage")
  private static final Type MAP_STRING_STRING_TYPE = new TypeToken<Map<String, String>>() {
  }.getType();
  private static final List<String> LOCATION_PROPERTIES = Arrays.asList("type", "host", "path", "user", "password");

  @Macro
  @Nullable
  @Description("Name be used to uniquely identify this source for lineage, annotating metadata, etc.")
  private final String referenceName;

  @Macro
  @Description("Whether to read from a FTP or SFTP server.")
  private final String type;

  @Macro
  @Description("Host to read from.")
  private final String host;

  @Macro
  @Nullable
  @Description("Optional port to read from. If none is given, this will default to 21 for FTP and 22 for SFTP.")
  private final Integer port;

  @Macro
  @Description("Path to the file or directory to read.")
  private final String path;

  @Macro
  @Description("User name to use for authentication.")
  private final String user;

  @Macro
  @Description("Password to use for authentication.")
  private final String password;

  @Macro
  @Nullable
  @Description("Any additional properties to use when reading from the filesystem. "
    + "This is an advanced feature that requires knowledge of the properties supported by the underlying filesystem.")
  private final String fileSystemProperties;

  @Macro
  @Nullable
  @Description("Whether to allow an input that does not exist. When false, the source will fail the run if the input "
    + "does not exist. When true, the run will not fail and the source will not generate any output. "
    + "The default value is false.")
  private final Boolean ignoreNonExistingFolders;

  @Macro
  @Nullable
  @Description("Regular expression that file names must match in order to be read. "
    + "If no value is given, no file filtering will be done. "
    + "See https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html for more information about "
    + "the regular expression syntax.")
  private final String fileRegex;

  @Macro
  @Nullable
  @Description("Whether to use first row as header. Supported formats are 'text', 'csv', 'tsv', " +
    "'delimited'. Default value is false.")
  private final Boolean skipHeader;

  @Macro
  @Description("Format of the data to read. Supported formats are 'avro', 'blob', 'csv', 'delimited', 'json', "
    + "'parquet', 'text', or 'tsv'. If no format is given, it will default to 'text'.")
  private final String format;

  @Macro
  @Nullable
  @Description("Output schema for the source. Formats like 'avro' and 'parquet' require a schema in order to "
    + "read the data.")
  private final String schema;

  @Macro
  @Nullable
  @Description("The delimiter to use if the format is 'delimited'. The delimiter will be ignored if the format "
    + "is anything other than 'delimited'.")
  private final String delimiter;

  @Macro
  @Nullable
  @Description("Whether to treat content between quotes as a value. This value will only be used if the format " +
    "is 'csv', 'tsv' or 'delimited'. The default value is false.")
  private final Boolean enableQuotedValues;

  @Macro
  @Nullable
  @Description("Enable the support for a single field, enclosed in quotes, to span over multiple lines. This " +
    "value will only be used if the format is 'csv', 'tsv' or 'delimited'. The default value is false.")
  protected Boolean enableMultilineSupport;

  @Macro
  @Nullable
  @Description("Maximum time in milliseconds to wait for connection initialization before time out.")
  private final Integer connectTimeout;

  @VisibleForTesting
  private FTPConfig(@Nullable String referenceName, String type, String host, @Nullable Integer port, String path,
                    String user, String password, @Nullable String fileSystemProperties,
                    @Nullable Boolean ignoreNonExistingFolders, @Nullable String fileRegex,
                    @Nullable Boolean skipHeader, @Nullable String format, @Nullable String schema,
                    @Nullable String delimiter, @Nullable Boolean enableQuotedValues,
                    @Nullable Boolean enableMultilineSupport, @Nullable Integer connectTimeout) {
    this.referenceName = referenceName;
    this.type = type;
    this.host = host;
    this.port = port;
    this.path = path;
    this.user = user;
    this.password = password;
    this.fileSystemProperties = fileSystemProperties;
    this.ignoreNonExistingFolders = ignoreNonExistingFolders;
    this.fileRegex = fileRegex;
    this.skipHeader = skipHeader;
    this.format = format;
    this.schema = schema;
    this.delimiter = delimiter;
    this.enableQuotedValues = enableQuotedValues;
    this.enableMultilineSupport = enableMultilineSupport;
    this.connectTimeout = connectTimeout;
  }

  public int getConnectTimeout() {
    if (connectTimeout == null) {
      return FTPFileSystem.DEFAULT_CONNECTION_TIMEOUT_MS;
    }
    return connectTimeout;
  }

  @Override
  public void validate(FailureCollector collector) {
    FTPLocation location = getLocation(collector);
    if (location == null) {
      return;
    }

    try {
      Job job = JobUtils.createInstance();
      Configuration conf = job.getConfiguration();
      for (Map.Entry<String, String> entry : getFileSystemProperties().entrySet()) {
        conf.set(entry.getKey(), entry.getValue());
      }
      for (Map.Entry<String, String> entry : location.getHadoopProperties().entrySet()) {
        conf.set(entry.getKey(), entry.getValue());
      }
      conf.setInt(FTPFileSystem.FS_CONNECT_TIMEOUT, getConnectTimeout());
      try (FileSystem fs = JobUtils.applyWithExtraClassLoader(job, getClass().getClassLoader(),
                                                              f -> FileSystem.get(location.getURI(), conf))) {
        fs.getFileStatus(new Path(location.getURI()));
      }
    } catch (Exception e) {
      //Log exception details as otherwise we lose it, and it's hard to debug
      LOG.warn("Unable to connect with the given url", e);
      collector.addFailure("Unable to connect with given url: " + e.getMessage(), null)
        .withStacktrace(e.getStackTrace());
    }
  }

  @Override
  public String getReferenceName() {
    return referenceName;
  }

  @Override
  public String getPath() {
    FTPLocation location = getLocation(null);
    return location != null ? location.getURI().toString() : null;
  }

  @Override
  public String getFormatName() {
    return format == null || format.isEmpty() ? FileFormat.TEXT.name().toLowerCase() : format;
  }

  @Nullable
  @Override
  public Pattern getFilePattern() {
    return Strings.isNullOrEmpty(fileRegex) ? null : Pattern.compile(fileRegex);
  }

  @Override
  public long getMaxSplitSize() {
    return Long.MAX_VALUE;
  }

  @Override
  public boolean shouldAllowEmptyInput() {
    return ignoreNonExistingFolders != null && ignoreNonExistingFolders;
  }

  @Override
  public boolean shouldReadRecursively() {
    return false;
  }

  @Nullable
  @Override
  public String getPathField() {
    return null;
  }

  @Override
  public boolean useFilenameAsPath() {
    return false;
  }

  @Override
  public boolean skipHeader() {
    return skipHeader != null && skipHeader;
  }

  @Nullable
  public Schema getSchema() {
    try {
      return containsMacro("schema") || Strings.isNullOrEmpty(schema) ? null : Schema.parseJson(schema);
    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid schema: " + e.getMessage(), e);
    }
  }

  /**
   * @return the location to read from. Returns null if there are macros so the location is unknown,
   * or if the location is invalid.
   */
  @Nullable
  FTPLocation getLocation(@Nullable FailureCollector collector) {
    if (LOCATION_PROPERTIES.stream().anyMatch(this::containsMacro)) {
      return null;
    }

    // try building the URI piece by piece in case there are invalid components. Then we can highlight
    // which part is problematic. First check host
    URI locationURI;
    FTPLocation.Type ftpType = getServerType(type);
    try {
      locationURI = new URI(ftpType.getScheme(), null, host, ftpType.getDefaultPort(), null, null, null);
    } catch (URISyntaxException e) {
      if (collector == null) {
        throw new IllegalArgumentException("Invalid host: " + e.getMessage(), e);
      }
      collector.addFailure("Invalid host: " + e.getMessage(), null)
        .withConfigProperty("host");
      return null;
    }

    // next check port
    if (port != null) {
      try {
        locationURI = UriBuilder.fromUri(locationURI).port(port).build();
      } catch (Exception e) {
        if (collector == null) {
          throw new IllegalArgumentException("Invalid port: " + port, e);
        }
        collector.addFailure("Invalid port: " + port, null)
          .withConfigProperty("port");
        return null;
      }
    }

    // next check path
    try {
      locationURI = UriBuilder.fromUri(locationURI).path(path).build();
    } catch (Exception e) {
      // not sure if this can actually happen, (UriBuilder will encode unsupported characters),
      // but here for completeness.
      if (collector == null) {
        throw new IllegalArgumentException("Invalid path: " + e.getMessage(), e);
      }
      collector.addFailure("Invalid path: " + e.getMessage(), null)
        .withConfigProperty("path");
      return null;
    }

    return new FTPLocation(ftpType, locationURI, user, password);
  }

  /**
   * Add failures to the collector for the individual components (host, port, path) that are invalid.
   */
  void handleInvalidURI(FailureCollector collector) {
    if (host == null) {
      return;
    }
    try {
      new URI(host);
    } catch (Exception e) {
      String errorReason = String.format("Unable to create a new URI for host %s", host);
      String errorMessage = String.format("Failed to create a new URi for host %s with reason %s.",
        host, e.getMessage());
      throw ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN),
        errorReason, errorMessage, ErrorType.SYSTEM, true, e);
    }
  }

  FTPLocation.Type getServerType(String type) {
    try {
      return FTPLocation.Type.valueOf(type.toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Invalid server type " + type + ". Specify either 'ftp' or 'sftp'");
    }
  }

  Map<String, String> getFileSystemProperties() {
    if (Strings.isNullOrEmpty(fileSystemProperties)) {
      return Collections.emptyMap();
    }
    return GSON.fromJson(fileSystemProperties, MAP_STRING_STRING_TYPE);
  }

  @VisibleForTesting
  static Builder builder() {
    return new Builder();
  }

  @SuppressWarnings("unused")
  @VisibleForTesting
  static class Builder {
    private FTPLocation.Type type;
    private String host;
    private String path;
    private String user;
    private String password;
    private String referenceName;
    private String fileRegex;
    private String format;
    private String delimiter;
    private int port;
    private Schema schema;
    private final Map<String, String> fileSystemProperties;
    private boolean ignoreNonExistingFolders;
    private boolean skipHeader;
    private boolean enableQuotedValues;
    private boolean enableMultilineSupport;
    private Integer connectTimeout;

    Builder() {
      this.fileSystemProperties = new HashMap<>();
      this.ignoreNonExistingFolders = false;
      this.enableQuotedValues = false;
      this.enableMultilineSupport = false;
      this.skipHeader = false;
      this.format = "text";
      this.connectTimeout = FTPFileSystem.DEFAULT_CONNECTION_TIMEOUT_MS;
    }

    Builder setType(FTPLocation.Type type) {
      this.type = type;
      return this;
    }

    Builder setHost(String host) {
      this.host = host;
      return this;
    }

    Builder setPort(int port) {
      this.port = port;
      return this;
    }

    Builder setPath(String path) {
      this.path = path;
      return this;
    }

    Builder setUser(String user) {
      this.user = user;
      return this;
    }

    Builder setPassword(String password) {
      this.password = password;
      return this;
    }

    Builder setReferenceName(String referenceName) {
      this.referenceName = referenceName;
      return this;
    }

    Builder setFileSystemProperties(Map<String, String> fileSystemProperties) {
      this.fileSystemProperties.clear();
      this.fileSystemProperties.putAll(fileSystemProperties);
      return this;
    }

    Builder setFileRegex(String regex) {
      this.fileRegex = regex;
      return this;
    }

    Builder setFormat(String format) {
      this.format = format;
      return this;
    }

    Builder setSchema(Schema schema) {
      this.schema = schema;
      return this;
    }

    Builder setDelimiter(String delimiter) {
      this.delimiter = delimiter;
      return this;
    }

    Builder setIgnoreExistingFolders(boolean ignoreExistingFolders) {
      this.ignoreNonExistingFolders = ignoreExistingFolders;
      return this;
    }

    Builder setSkipHeader(boolean skipHeader) {
      this.skipHeader = skipHeader;
      return this;
    }

    Builder setEnableQuotedValues(boolean enableQuotedValues) {
      this.enableQuotedValues = enableQuotedValues;
      return this;
    }

    Builder setEnableMultilineSupport(boolean enableMultilineSupport) {
      this.enableMultilineSupport = enableMultilineSupport;
      return this;
    }

    Builder setConnectTimeout(Integer connectTimeout) {
      this.connectTimeout = connectTimeout;
      return this;
    }

    FTPConfig build() {
      return new FTPConfig(referenceName, type.name(), host, port, path, user, password,
                           GSON.toJson(fileSystemProperties), ignoreNonExistingFolders, fileRegex, skipHeader,
                           format, schema == null ? null : schema.toString(), delimiter, enableQuotedValues,
                           enableMultilineSupport, connectTimeout);
    }
  }
}
