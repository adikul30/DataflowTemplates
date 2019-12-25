package com.google.cloud.teleport.templates;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

public class BigQueryToPubSub {
    static class RowToString extends DoFn<TableRow, String> {
        @ProcessElement
        public void processElement(DoFn<TableRow, String>.ProcessContext c) {
            c.output(c.element().toString());
        }
    }

    public static void main(String[] args) {
        Pipeline p = Pipeline.create(PipelineOptionsFactory.fromArgs(args).withValidation().create());

        p.apply(BigQueryIO.readTableRows().from("bigquery-public-data:github_repos.commits"))
                .apply(ParDo.of(new RowToString()))
                .apply(PubsubIO.writeStrings().to("projects/durable-trainer-256515/topics/bigquerytopic"));

        p.run();
    }
}
