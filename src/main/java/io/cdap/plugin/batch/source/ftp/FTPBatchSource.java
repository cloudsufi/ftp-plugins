/*
 * Copyright © 2016-2023 Cask Data, Inc.
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

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.cdap.etl.api.exception.ErrorDetailsProviderSpec;
import io.cdap.plugin.common.Asset;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.common.ReferenceNames;
import io.cdap.plugin.format.plugin.AbstractFileSource;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * {@link BatchSource} that reads from an FTP or SFTP server.
 */
@Plugin(type = "batchsource")
@Name(FTPBatchSource.NAME)
@Description("Reads data from an FTP or SFTP server.")
public class FTPBatchSource extends AbstractFileSource<FTPConfig> {
  public static final String NAME = "FTPSource";
  private final FTPConfig config;
  private Asset asset;

  public FTPBatchSource(FTPConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    // create asset for lineage
    String referenceName = Strings.isNullOrEmpty(config.getReferenceName())
      ? ReferenceNames.normalizeFqn(config.getPath())
      : config.getReferenceName();
    asset = Asset.builder(referenceName)
      .setFqn(config.getPath()).build();

    // set error details provider
    context.setErrorDetailsProvider(new ErrorDetailsProviderSpec(FTPErrorDetailsProvider.class.getName()));

    // super is called down here to avoid instantiating the lineage recorder with a null asset
    super.prepareRun(context);
  }

  @Override
  protected Map<String, String> getFileSystemProperties(@Nullable BatchSourceContext context) {
    FailureCollector failureCollector = context == null ? null : context.getFailureCollector();
    FTPLocation location = config.getLocation(failureCollector);
    if (location == null && failureCollector != null) {
      failureCollector.getOrThrowException();
    }
    Map<String, String> properties = new HashMap<>(config.getFileSystemProperties());
    properties.putAll(location.getHadoopProperties());
    return properties;
  }

  @Override
  protected LineageRecorder getLineageRecorder(BatchSourceContext context) {
    return new LineageRecorder(context, asset);
  }

}
