package edu.usfca.dataflow;


import com.google.api.services.bigquery.model.TableReference;
import edu.usfca.dataflow.jobs1.BidLogJob;
import edu.usfca.dataflow.jobs2.PredictionJob;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Read the instructions provided in Google Docs, before you begin working on this project.
 */
public class Main {
    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    // TODO: Make sure you change the following four to your own settings.
    public final static String GCP_PROJECT_ID = "cs6868-proj-4-5-6"; // <- TODO change this to your GCP project ID.
    public final static String GCS_BUCKET = "gs://bucket-686"; // <- TODO change this to your bucket (that you used
    // for project 4).
    public final static String REGION = "us-west1"; // Don't change this.

    public final static String MY_BQ_DATASET = "proj05_dataset"; // <- TODO change this to an existing dataset in your BigQuery.
    public final static String MY_BQ_TABLE = "my_proj5_table"; // <- this can be anything, and it'll be auto-created.

    public final static TableReference DEST_TABLE =
            new TableReference().setProjectId(GCP_PROJECT_ID).setDatasetId(MY_BQ_DATASET).setTableId(MY_BQ_TABLE);

    // TODO: Change the following to the local path directory that contains the downloaded resource files.
    // It's recommended that you provide the absolute path here (not relative path)!
    // Note that, in this directory, "input/model" directories must be found (or the job will throw an exception).
    public final static String LOCAL_PATH_TO_RESOURCE_DIR = "/Users/ayusharora/Downloads/project5/project5-actual";

    // TODO: Change the following to the GCS path that contains the resource files.
    // Note that (when you run jobs on GCP) you can override this by feeding the command-line argument.
    // Note that, in this directory, "input/model" directories must be found (or the job will throw an exception).
    public final static String GCS_PATH_TO_RESOURCE_DIR = GCS_BUCKET + "/resources/project5-small-data";

    // NOTE: You will not need to run the jobs through the main() method until you complete Tasks A & B.
    // Yet, you can still run them locally (which is the default behavior) to ensure your pipeline runs without errors.
    public static void main(String[] args) {
        // TODO Take a look at MyOptions class in order to understand what command-line flags are available.
        MyOptions options = PipelineOptionsFactory.fromArgs(args).as(MyOptions.class);

        final String job = options.getJob();
        switch (job) {
            case "bidLogJob": // Pipeline #1 for Task A
                options = setDefaultValues(options);
                BidLogJob.execute(options);
                break;

            case "predictionJob": // Pipeline #2 for Task B
                options = setDefaultValues(options);
                PredictionJob.execute(options);
                break;

            default: // Should not be reached.
                System.out.println("unknown job flag: " + job);
                break;
        }
    }

    /**
     * This sets default values for the Options class so that when you run your job on GCP, it won't complain about
     * missing parameters.
     * <p>
     * You don't have to change anything here.
     */
    static MyOptions setDefaultValues(MyOptions options) {
//        System.out.format("user.dir: %s", System.getProperty("user.dir"));

        options.setJobName(String.format("%s-%05d", options.getJob(), org.joda.time.Instant.now().getMillis() % 100000));

        options.setTempLocation(GCS_BUCKET + "/staging");
        if (options.getIsLocal()) {
            options.setRunner(DirectRunner.class);
        } else {
            options.setRunner(DataflowRunner.class);
        }
        if (options.getMaxNumWorkers() == 0) {
            options.setMaxNumWorkers(1);
        }
        if (StringUtils.isBlank(options.getWorkerMachineType())) {
            options.setWorkerMachineType("n1-standard-1");
        }
        options.setDiskSizeGb(150);
        options.setRegion(REGION);
        options.setProject(GCP_PROJECT_ID);

        // You will see more info here.
        // To run a pipeline (job) on GCP via Dataflow, you need to specify a few things like the ones above.
//        LOG.info(options.toString());

        return options;
    }
}
