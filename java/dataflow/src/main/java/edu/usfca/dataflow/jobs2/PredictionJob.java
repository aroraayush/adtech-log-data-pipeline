package edu.usfca.dataflow.jobs2;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.protobuf.InvalidProtocolBufferException;
import edu.usfca.dataflow.Main;
import edu.usfca.dataflow.MyOptions;
import edu.usfca.dataflow.transforms.Features.GetInputToModel;
import edu.usfca.dataflow.transforms.Predictions;
import edu.usfca.dataflow.utils.PathConfigs;
import edu.usfca.dataflow.utils.ProtoUtils;
import edu.usfca.protobuf.Common.DeviceId;
import edu.usfca.protobuf.Data.PredictionData;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.Arrays;
import java.util.List;

public class PredictionJob {

    /**
     * ---------------------------------------------------------------
     * <p>
     * Instructions (Task B)
     * <p>
     * ---------------------------------------------------------------
     * <p>
     * 1. Before you begin writing any code, run unit tests found in java/judge.
     * <p>
     * Specifically, pay attention to all unit tests in "__Test11*"-"__Test16*" classes.
     * <p>
     * Read the comments found in each of those files, as they give you an idea of what you should work on to pass them.
     * <p>
     * 1-1. "__Test11PredictionUtils" All tests in this class must pass without changing the starter code.
     * <p>
     * 1-2. "__Test12PredictionJob" All tests in this class must pass without changing the starter code.
     * <p>
     * 1-3. "__Test13PredictionJob" This checks if you implemented the second pipeline correctly or not, using small
     * examples.
     * <p>
     * 1-4. "__Test14PredictionJob" This checks if you implemented the second pipeline correctly or not, using small
     * examples.
     * <p>
     * 1-5. "__Test15" and "__Test16": These tests are similar to "__Test14" but with larger examples. You will need to
     * optimize PredictDoFn in order to pass these tests (otherwise, your pipeline will "timeout"). Make sure you apply
     * the optimization techniques we learned in class (figuring out which ones to apply is the part of the assignment).
     * <p>
     * 2. As you implement "GetInputToModel" PTransform, you must now check the unit test results in
     * "__Test13PredictionJob" class.
     * <p>
     * 3. Optimize PredictDoFn using the optimization techniques we learned in class.
     * <p>
     * 4. When you are ready to execute your pipeline (from local machine), do so. It will produce output files (JSON) as
     * well as a BigQuery table (if you set things up correctly in Main). Check the contents of the generated table, and
     * compare that with the reference answers (see README).
     * <p>
     * 4-1. Run the following command under "java/dataflow" directory:
     * <p>
     * "gradle run -Pargs="--job=predictionJob --pathToResourceRoot=/Users/ayusharora/Downloads/project5-actual""
     * gradle run -Pargs="--job=predictionJob --pathToResourceRoot=gs://bucket-686/project5-small-data --exportToBigQuery=false  --isLocal=false --numWorkers=2 --maxNumWorkers=2 --workerMachineType=n1-standard-8"
     * gradle run -Pargs="--job=predictionJob --pathToResourceRoot=gs://bucket-686/project5-small-data --exportToBigQuery=true  --isLocal=false --numWorkers=2 --maxNumWorkers=2 --workerMachineType=n1-standard-8"
     * <p>
     * It should produce one output file (output/prediction-data/result-00000-of-00001.json). Compare the contents of your
     * file with the reference solution (see the instructions document).
     * <p>
     * 4-2. If they match, then run it one more time, with the following command.
     * <p>
     * "gradle run -Pargs="--job=predictionJob --pathToResourceRoot=/Users/ayusharora/Downloads/project5-actual --exportToBigQuery=true""
     * <p>
     * The last flag ensures that the output will be written to BigQuery table (which you configured in Main method).
     * <p>
     * As a final check, make sure the table contains the 46 rows you expect (compare with the reference solution).
     * <p>
     * In Task C, you will run your job on GCP, and your score for Task C will be based on the efficiency of your
     * pipeline. Further instructions will be provided after you complete Tasks A and B.
     */

    final static List<TableFieldSchema> BQ_FIELDS = Arrays.asList(//
            new TableFieldSchema().setName("os").setType("STRING"), //
            new TableFieldSchema().setName("uuid").setType("STRING"), //
            new TableFieldSchema().setName("prediction").setType("INTEGER"), //
            new TableFieldSchema().setName("score").setType("FLOAT"));

    public static void execute(MyOptions options) {
        final PathConfigs config = PathConfigs.of(options);
        Pipeline p = Pipeline.create(options);

        // 1. Read "DeviceProfile" "Suspicious User" and "InAppPurchaseProfile" data.
        // The first two are supposed to be the output of your first pipeline (BidLogJob).
        // The second is provided (= supposed to be the output of your pipeline from project 3/4).
        PCollection<String> dpBase64 = p.apply("ReadDeviceProfile",TextIO.read().from(config.getReadPathToDeviceProfile()));
        PCollection<String> suspiciousBase64 = p.apply("ReadSuspiciousUser",TextIO.read().from(config.getReadPathToSuspiciousUser()));
        PCollection<String> iappBase64 = p.apply("ReadIAPP",TextIO.read().from(config.getReadPathToIAPP()));

        // 2. One big PTransform that prepares the input data for ML model.
        PCollection<KV<DeviceId, float[]>> inputToModel =
                PCollectionList.of(dpBase64).and(suspiciousBase64).and(iappBase64).apply("GetInputToModel",new GetInputToModel());

        // 3. Make predictions.
        // PredictDoFn is already provided, but it is not efficient.
        // Based on what we studied in class, you should optimize it.
        PCollection<PredictionData> predictions =
                inputToModel.apply("getPredictDoFn",ParDo.of(Predictions.getPredictDoFn(config.getPathToModel())));

        // 4. Write results to GCS as well as to BigQuery.
        PCollection<String> predictionJson =
                predictions.apply(MapElements.into(TypeDescriptors.strings()).via((PredictionData data) -> {
                    try {
                        return ProtoUtils.getJsonFromMessage(data, true);
                    } catch (InvalidProtocolBufferException e) {
                        e.printStackTrace();
                    }
                    return null;
                }));
        predictionJson.apply("TextIOWritePredictions",TextIO.write().to(config.getWritePathToPredictionData()).withNumShards(1));

        // 5. Write to BigQuery.
        if (options.getExportToBigQuery()) {

            predictions.apply("BigQueryIO_Write_PredictionData",BigQueryIO.<PredictionData>write().to(Main.DEST_TABLE)//
                    .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)// <- TODO what other options are available?
                    .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED) // <- TODO what other options are available?
                    .withSchema(new TableSchema().setFields(BQ_FIELDS))//
                    .withFormatFunction((SerializableFunction<PredictionData, TableRow>) input//
                            -> new TableRow().set("os", input.getId().getOs()).set("uuid", input.getId().getUuid()).set("prediction", input.getPrediction())
                            .set("score", input.getScore())));
            // Make sure you are using "Main.DEST_TABLE" and "PredictionJob.BQ_FIELDS" in your code.
        }

        p.run().waitUntilFinish();
    }
}
