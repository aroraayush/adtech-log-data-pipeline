package edu.usfca.dataflow.transforms;

import edu.usfca.protobuf.Common.DeviceId;
import edu.usfca.protobuf.Profile.AppProfile;
import edu.usfca.protobuf.Profile.DeviceProfile;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SuspiciousIDs {

    private static final Logger LOG = LoggerFactory.getLogger(SuspiciousIDs.class);

    /**
     * This method serves to flag certain users as suspicious.
     * <p>
     * (1) USER_COUNT_THRESHOLD: This determines whether an app is popular or not.
     * <p>
     * Any app that has more than "USER_COUNT_THRESHOLD" unique users is considered popular.
     * <p>
     * Default value is 4 (so, 5 or more users = popular).
     * <p>
     * <p>
     * (2) APP_COUNT_THRESHOLD: If a user (DeviceProfile) contains more than "APP_COUNT_THRESHOLD" unpopular apps,
     * <p>
     * then the user is considered suspicious.
     * <p>
     * Default value is 3 (so, 4 or more unpopular apps = suspicious).
     * <p>
     * <p>
     * (3) GEO_COUNT_THRESHOLD: If a user (DeviceProfile) contains more than "GEO_COUNT_THRESHOLD" unique Geo's,
     * <p>
     * then the user is considered suspicious.
     * <p>
     * Default value is 8 (so, 9 or more distinct Geo's = suspicious).
     * <p>
     * <p>
     * (4) BID_LOG_COUNT_THRESHOLD: If a user (DeviceProfile) appeared in more than "BID_LOG_COUNT_THRESHOLD" Bid Logs,
     * <p>
     * then the user is considered suspicious (we're not counting invalid BidLogs for this part as it should have been
     * ignored from the beginning).
     * <p>
     * Default value is 10 (so, 11 or more valid BidLogs from the same user = suspicious).
     * <p>
     * <p>
     * NOTE: When you run your pipelines on GCP, we'll not use the default values for these thresholds (see the document).
     * <p>
     * The default values are mainly for unit tests (so you can easily check correctness with rather small threshold
     * values).
     */

  public static PCollection<DeviceId> getSuspiciousIDs(PCollection<DeviceProfile> dps, PCollection<AppProfile> aps,
     int USER_COUNT_THRESHOLD, int APP_COUNT_THRESHOLD, int GEO_COUNT_THRESHOLD, int BID_LOG_COUNT_THRESHOLD ) {

        // Get unpopular apps as side input
        PCollectionView<List<String>> unpopularAppsView = aps.apply("getUnpopularApps",
                ParDo.of(new DoFn<AppProfile, String>() {
            // Any app that has more than "USER_COUNT_THRESHOLD" unique users is considered popular.
            @ProcessElement
            public void process(@Element AppProfile input, OutputReceiver<String> out) {
                if (input.getUserCount() > USER_COUNT_THRESHOLD) {
                    out.output(input.getBundle());
                }

            }
        })).apply("unpopularAppsView",View.asList());

        return dps.apply("getSuspiciousUsers",ParDo.of(new DoFn<DeviceProfile, DeviceId>() {
            Set<String> unpopularApps;
            @ProcessElement
            public void process(ProcessContext c) {
                if (unpopularApps == null) {
                    unpopularApps = new HashSet<>(c.sideInput(unpopularAppsView));
                }
                if(c.element().getGeoCount() > GEO_COUNT_THRESHOLD){
                    c.output(c.element().getDeviceId());
                    LOG.info("GEO {}", c.element().getDeviceId());
                    return;
                }
                int bidLogCnt = 0;

                    int appCount = 0;
                    for (DeviceProfile.AppActivity app : c.element().getAppList()) {
                        if (!unpopularApps.contains(app.getBundle()))
                            appCount++;
                        if (appCount > APP_COUNT_THRESHOLD) {
                            c.output(c.element().getDeviceId());
                            LOG.info("APP {}", c.element().getDeviceId());
                            return;
                        }
                        for (Integer count : app.getCountPerExchangeMap().values()) {
                            bidLogCnt += count;
                            if(bidLogCnt > BID_LOG_COUNT_THRESHOLD) {
                                LOG.info("BID {}", c.element().getDeviceId());
                                c.output(c.element().getDeviceId());
                                return;
                            }
                        }
                    }
            }
        }).withSideInputs(unpopularAppsView));
    }
}
