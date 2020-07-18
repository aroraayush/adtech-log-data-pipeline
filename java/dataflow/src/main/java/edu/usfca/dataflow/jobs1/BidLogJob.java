package edu.usfca.dataflow.jobs1;

import com.google.protobuf.InvalidProtocolBufferException;
import edu.usfca.dataflow.MyOptions;
import edu.usfca.dataflow.transforms.AppProfiles.ComputeAppProfiles;
import edu.usfca.dataflow.utils.BidLogUtils;
import edu.usfca.dataflow.utils.DeviceProfileUtils;
import edu.usfca.dataflow.utils.IOUtils;
import edu.usfca.dataflow.utils.PathConfigs;
import edu.usfca.protobuf.Bid;
import edu.usfca.protobuf.Common;
import edu.usfca.protobuf.Profile.AppProfile;
import edu.usfca.protobuf.Profile.DeviceProfile;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TFRecordIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static edu.usfca.dataflow.transforms.SuspiciousIDs.getSuspiciousIDs;

public class BidLogJob {
  private static final Logger LOG = LoggerFactory.getLogger(BidLogJob.class);

  /**
   * ---------------------------------------------------------------
   *
   * Instructions (Task A)
   *
   * ---------------------------------------------------------------
   *
   * 1. Before you begin writing any code, run unit tests found in java/judge.
   *
   * Specifically, pay attention to all unit tests in "__Test01*"-"__Test09*" classes (for Task A).
   *
   * Read the comments found in each of those files, as they give you an idea of what you should work on to pass them.
   *
   * 1-1. "__Test01BidLogUtils" Fix silly bugs in BidLogUtils class.
   *
   * 1-2. "__Test02DeviceProfileUtils" Fix silly bugs in DeviceProfileUtils class.
   *
   * 2. As you work on PTransforms, you will repeat what you did in Project 2. Note that the initial input (BidLogs) is
   * given in TFRecord format, and you should review L31 (sample code) for using TFRecordIO. Given the starter code, you
   * really do not need to write a lot of code (unless you want to write everything from scratch). Instead of writing
   * your own, I strongly suggest you use the provided starter code as a starting point (correctness first, optimization
   * later).
   *
   * 2-1. "__Test03BidLogJob" This tests BidLog to DeviceProfiles.
   *
   * 2-2. "__Test04BidLogJob" This tests AppProfiles.
   *
   * 2-3. "__Test05BidLogJob" This tests Suspicious Users.
   *
   * 2-4. "__Test06BidLogJob" This tests Suspicious Users.
   *
   * 2-5. "__Test07" through "__Test09" use many BidLogs to test the entire pipeline (all outputs). If your pipeline
   * passes all unit tests up to __Test06 but fails on __Test07-09, perhaps there's a subtle bug in your pipeline (which
   * may lead to non-deterministic behaviors). Make sure that you debug your pipeline using IDE's debugger & by printing
   * messages/data to the console.
   *
   * 2-6. If all unit tests pass, run your job locally (using DirectRunner) by using the following command:
   *
   * Under "java/dataflow" directory (of your local repo):
   *
   * gradle run -Pargs="--job=bidLogJob --bidLogCountThreshold=115 --geoCountThreshold=1 --userCountThreshold=2 --appCountThreshold=18 --pathToResourceRoot=/Users/ayusharora/Downloads/project5-actual""
   *
   * (Note that the last flag can be omitted if that's the path you already set in Main method.)
   * gradle run -Pargs="--job=bidLogJob --pathToResourceRoot=gs://bucket-686/project5-small-data --bidLogCountThreshold=128 --geoCountThreshold=5 --userCountThreshold=10000 --appCountThreshold=15 --isLocal=false --numWorkers=2 --maxNumWorkers=2 --workerMachineType=n1-standard-8"
   * gradle run -Pargs="--job=bidLogJob --pathToResourceRoot=gs://bucket-686/project5-large-data --bidLogCountThreshold=128 --geoCountThreshold=5 --userCountThreshold=10000 --appCountThreshold=15 --isLocal=false --numWorkers=2 --maxNumWorkers=2 --workerMachineType=n1-standard-8"
   *
   * 3. When this job runs successfully (either on your machine or on GCP), it should output three files, one file under
   * each subdirectory of the "output" directory of your LOCAL_PATH_TO_RESOURCE_DIR (if you ran it locally).
   *
   * 3-1. DeviceProfile: Merged DeviceProfile data (per user=DeviceId). We'll treat this dataset as "lifetime"
   * DeviceProfile dataset. Revisit Project 2 for the details (or see DeviceProfileUtils class). You should see 707
   * lines in the file.
   *
   * 3-2. AppProfile: AppProfile data (generated using DeviceProfile data from 3-1). To simplify things, we are only
   * counting lifetime user count and also count per exchange. You should see 510 lines in the file.
   *
   * 3-3. Suspicious (Device): If a certain device has *a lot of apps* that few people use, the said device may be a bot
   * (not a normal human user). In this project, a DeviceId is considered "suspicious" if it has enough unpopular apps
   * (where an app is unpopular if the unique number of users is small). In addition, if a device "appeared" in too many
   * geological locations, that's also considered suspicious. You should see 6 lines in the file.
   *
   * 3-4. For all three datasets mentioned above, you'll write text files in Base64 encoding. Some of these will be used
   * in the second job (pipeline), called PredictionJob.
   *
   * Note that "sample output" files are provided (in the "output-reference" directory), as they should be used as input
   * to the second pipeline, but the contents of the sample output could be different from the contents of your output
   * (e.g., the order of the lines can be different as PCollections do not preserve the order of elements).
   *
   * 4. Before you begin to optimize your "BidLog2DeviceProfile" PTransform, you must ensure that all unit tests pass.
   *
   * In Task C, you will run your job on GCP, and your score for Task C will be based on the efficiency of your
   * pipeline. Further instructions will be provided after you complete Task A.
   */

  public static class BidLog2DeviceProfile extends PTransform<PCollection<byte[]>, PCollection<DeviceProfile>> {

    @Override
    public PCollection<DeviceProfile> expand(PCollection<byte[]> bidLogBinary) {
      // TODO: Your first step should be to decode "byte[]" using <proto>.parseFrom to obtain proto messages.
      // Note that the input PCollection contains BidLog protos (but serializeD).

      return bidLogBinary.apply("DevProfFromBidLog",ParDo.of(new DoFn<byte[], KV<String,DeviceProfile>>() {
        @ProcessElement
        public void process(@Element byte[] binaryLog, OutputReceiver<KV<String,DeviceProfile>> out) {
          try{
            DeviceProfile deviceProfile = BidLogUtils.getDeviceProfile(Bid.BidLog.parseFrom(binaryLog));
            out.output(KV.of(deviceProfile.getDeviceId().getOs()+"$"+deviceProfile.getDeviceId().getUuid().toUpperCase(),deviceProfile));
          }
          catch (InvalidProtocolBufferException | IllegalArgumentException e){
            System.out.println("Exception (Invalid Protobuf or IllegalArgument)" + e);
          }
        }
      })).apply("mergeCombinedDps",
        Combine.perKey((SerializableFunction<Iterable<DeviceProfile>, DeviceProfile>) DeviceProfileUtils::mergeDps))
              .apply("PC_DeviceProfile",Values.create());
    }
  }


  public static void execute(MyOptions options) {
//    LOG.info("Options: {}", options.toString());
    // ----------------------------------------------------------------------
    // TODO: You should NOT change what's in PathConfigs class, but DO take a look to understand how input/output paths
    // are decided in this project. Likewise, DO take a look at "MyOptions" class.
    final PathConfigs config = PathConfigs.of(options);
    Pipeline p = Pipeline.create(options);

    // 1. Read BidLog data from TFRecord files, create DeviceProfiles, and return merged DeviceProfiles.
    PCollection<byte[]> rawData = p.apply("ReadBase64BidLogs",TFRecordIO.read().from(config.getReadPathToBidLog()));
    PCollection<DeviceProfile> deviceProfiles = rawData.apply("BidLog2DeviceProfile",new BidLog2DeviceProfile());

    // 2. Obtain AppProfiles.
    PCollection<AppProfile> appProfiles = deviceProfiles.apply("DevProf_to_AppProf",new ComputeAppProfiles());

    // 3. Suspicious users (IDs).
    PCollection<Common.DeviceId> suspiciousUsers = getSuspiciousIDs(deviceProfiles, appProfiles,
        options.getUserCountThreshold(), options.getAppCountThreshold(), options.getGeoCountThreshold(),
        options.getBidLogCountThreshold());

    // 4. Output (write to GCS).
    // For convenience, we'll use Base64 encoding.
    // TODO: Uncomment the following, and use the right parameters (this will be necessary for Task C, in particular).
     IOUtils.encodeB64AndWrite(deviceProfiles, config.getWritePathToDeviceProfile());
     IOUtils.encodeB64AndWrite(appProfiles, config.getWritePathToAppProfile());
     IOUtils.encodeB64AndWrite(suspiciousUsers, config.getWritePathToSuspiciousUser());

    p.run().waitUntilFinish();
  }
}
