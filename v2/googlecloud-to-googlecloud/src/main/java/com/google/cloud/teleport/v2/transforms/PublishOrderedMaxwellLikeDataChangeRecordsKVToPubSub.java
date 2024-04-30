/*
 * Copyright (C) 2024 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.transforms;

import com.google.auto.value.AutoValue;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.DataChangeRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link PublishOrderedMaxwellLikeDataChangeRecordsKVToPubSub} class is a {@link PTransform}
 * that takes in {@link PCollection} of KV of BucketIndex and DataChangeRecords[]. The transform
 * writes each record of DataChangeRecords[] serially to PubsubMessage with BucketIndex as ordering
 * key.
 */
@AutoValue
public abstract class PublishOrderedMaxwellLikeDataChangeRecordsKVToPubSub
    extends PTransform<
        PCollection<KV<String, Iterable<DataChangeRecord>>>,
        PCollection<KV<String, Iterable<byte[]>>>> {

  /** Logger for class. */
  private static final Logger LOG =
      LoggerFactory.getLogger(PublishOrderedMaxwellLikeDataChangeRecordsKVToPubSub.class);

  public static WriteToPubSubBuilder newBuilder() {
    return new AutoValue_PublishOrderedMaxwellLikeDataChangeRecordsKVToPubSub.Builder();
  }

  protected abstract String outputDataFormat();

  protected abstract String projectId();

  protected abstract String pubsubTopicName();

  protected abstract String pubsubEndpoint();

  @Override
  public PCollection<KV<String, Iterable<byte[]>>> expand(
      PCollection<KV<String, Iterable<DataChangeRecord>>> recordsKV) {
    PCollection<KV<String, Iterable<byte[]>>> encodedRecordsKV = null;

    /*
     * Calls appropriate class Builder to performs PTransform based on user provided File Format.
     */
    switch (outputDataFormat()) {
      case "JSON":
        encodedRecordsKV =
            recordsKV.apply(
                "Deserialize DataChangeRecord to UTF-8 bytes",
                ParDo.of(
                    new DoFn<
                        KV<String, Iterable<DataChangeRecord>>, KV<String, Iterable<byte[]>>>() {
                      @ProcessElement
                      public void processElement(ProcessContext context) {
                        Gson gson = new Gson();
                        String key = context.element().getKey();
                        List<byte[]> encodedRecords = new ArrayList<>();
                        for (DataChangeRecord record : context.element().getValue()) {
                          MaxwellLikeDataChangeRecord transformedRecord =
                              transformSpannerToMaxwellDataChangeRecord(record);
                          String jsonRecord =
                              gson.toJson(transformedRecord, MaxwellLikeDataChangeRecord.class);
                          byte[] encodedRecord = jsonRecord.getBytes();
                          encodedRecords.add(encodedRecord);
                        }
                        context.outputWithTimestamp(
                            KV.of(context.element().getKey(), encodedRecords), context.timestamp());
                      }
                    }));
        sendToPubSub(encodedRecordsKV);
        break;

      default:
        final String errorMessage =
            "Invalid output format:" + outputDataFormat() + ". Supported output formats: JSON";
        LOG.info(errorMessage);
        throw new IllegalArgumentException(errorMessage);
    }
    return encodedRecordsKV;
  }

  private class MaxwellLikeDataChangeRecord {
    // private String database;
    private String table;
    private String type;
    private long ts;
    private String xid;
    private Boolean commit;
    private JsonObject data;

    // Constructor
    public MaxwellLikeDataChangeRecord(
        // String database,
        String table, String type, long ts, String xid, Boolean commit, JsonObject data) {
      // this.database = database;
      this.table = table;
      this.type = type;
      this.ts = ts;
      this.xid = xid;
      this.commit = commit;
      this.data = data;
    }
  }

  private JsonObject getSpannerChangeRecordValues(DataChangeRecord spannerRecord) {
    JsonObject keysJsonObject =
        JsonParser.parseString(spannerRecord.getMods().get(0).getKeysJson()).getAsJsonObject();
    JsonObject valuesJsonObject =
        JsonParser.parseString(spannerRecord.getMods().get(0).getNewValuesJson()).getAsJsonObject();
    JsonObject mergedJsonObject = new JsonObject();

    // Add all properties from keysJsonObject
    for (String key : keysJsonObject.keySet()) {
      mergedJsonObject.add(key, keysJsonObject.get(key));
    }

    // Merge properties
    for (String key : valuesJsonObject.keySet()) {
      mergedJsonObject.add(key, valuesJsonObject.get(key));
    }

    return mergedJsonObject;
  }

  private MaxwellLikeDataChangeRecord transformSpannerToMaxwellDataChangeRecord(
      DataChangeRecord spannerRecord) {
    return new MaxwellLikeDataChangeRecord(
        // NULL,
        spannerRecord.getTableName(),
        spannerRecord.getModType().toString().toLowerCase(),
        spannerRecord.getCommitTimestamp().getSeconds(),
        spannerRecord.getServerTransactionId(),
        spannerRecord.isLastRecordInTransactionInPartition(),
        getSpannerChangeRecordValues(spannerRecord));
  }

  private void sendToPubSub(PCollection<KV<String, Iterable<byte[]>>> encodedRecordsKV) {
    String pubsubTopicName = pubsubTopicName();
    String projectId = projectId();
    String pubsubEndpoint = pubsubEndpoint();
    String outputPubsubTopic = "projects/" + projectId + "/topics/" + pubsubTopicName;

    final PublishOrderedKVToPubSubDoFn publishOrderedKVToPubSubDoFn =
        new PublishOrderedKVToPubSubDoFn(projectId, pubsubTopicName, pubsubEndpoint);
    encodedRecordsKV.apply(ParDo.of(publishOrderedKVToPubSubDoFn));
  }

  /** Builder for {@link PublishOrderedMaxwellLikeDataChangeRecordsKVToPubSub}. */
  @AutoValue.Builder
  public abstract static class WriteToPubSubBuilder {

    public abstract WriteToPubSubBuilder setOutputDataFormat(String value);

    public abstract WriteToPubSubBuilder setProjectId(String value);

    public abstract WriteToPubSubBuilder setPubsubTopicName(String value);

    public abstract WriteToPubSubBuilder setPubsubEndpoint(String value);

    abstract PublishOrderedMaxwellLikeDataChangeRecordsKVToPubSub autoBuild();

    public PublishOrderedMaxwellLikeDataChangeRecordsKVToPubSub build() {
      return autoBuild();
    }
  }
}
