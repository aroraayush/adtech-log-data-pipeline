package edu.usfca.dataflow.transforms;

import com.google.protobuf.InvalidProtocolBufferException;
import edu.usfca.dataflow.CorruptedDataException;
import edu.usfca.dataflow.utils.DeviceProfileUtils;
import edu.usfca.dataflow.utils.PredictionUtils;
import edu.usfca.dataflow.utils.ProtoUtils;
import edu.usfca.protobuf.Common.DeviceId;
import edu.usfca.protobuf.Profile;
import edu.usfca.protobuf.Profile.DeviceProfile;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionView;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Features {
    /**
     * This PTransform takes a PCollectionList that contains three PCollections of Strings.
     * <p>
     * 1. DeviceProfile (output from the first pipeline) with unique DeviceIDs,
     * <p>
     * 2. DeviceId (output from the first pipeline) that are "suspicious" (call this SuspiciousIDs), and
     * <p>
     * 3. InAppPurchaseProfile (separately provided) with unique bundles.
     * <p>
     * All of these proto messages are Base64-encoded (you can check ProtoUtils class for how to decode that, e.g.).
     * <p>
     * [Step 1] First, in this PTransform, you must filter out (remove) DeviceProfiles whose DeviceIDs are found in the
     * SuspiciousIDs as we are not going to consider suspicious users.
     * <p>
     * [Step 2] Next, you ALSO filter out (remove) DeviceProfiles whose DeviceID's UUIDs are NOT in the following form:
     * <p>
     * ???????0-????-????-????-????????????
     * <p>
     * Effectively, this would "sample" the data at rate (1/16). This sampling is mainly for efficiency reasons (later
     * when you run your pipeline on GCP, the input data is quite large as you will need to make "predictions" for
     * millions of DeviceIDs).
     * <p>
     * To be clear, if " ...getUuid().charAt(7) == '0' " is true, then you process the DeviceProfile; otherwise, ignore
     * it.
     * <p>
     * [Step 3] Then, for each user (DeviceProfile), use the method in
     * {@link edu.usfca.dataflow.utils.PredictionUtils#getInputFeatures(DeviceProfile, Map)} to obtain the user's
     * "Features" (to be used for TensorFlow model). See the comments for this method.
     * <p>
     * Note that the said method takes in a Map (in addition to DeviceProfile) from bundles to IAPP, and thus you will
     * need to figure out how to turn PCollection into a Map. We have done this in the past (in labs & lectures).
     */
    public static class GetInputToModel extends PTransform<PCollectionList<String>, PCollection<KV<DeviceId, float[]>>> {

        @Override
        public PCollection<KV<DeviceId, float[]>> expand(PCollectionList<String> pcList) {

            PCollectionView<List<DeviceId>> blackListView = (pcList.get(1)).apply("SuspiciousDeviceIds",
                ParDo.of(new DoFn<String, DeviceId>() {
                    @ProcessElement
                    public void process(ProcessContext c) throws InvalidProtocolBufferException {
                        c.output(ProtoUtils.decodeMessageBase64(DeviceId.parser(), c.element()));
                    }

            })).apply("blackListViewAsList",View.asList());

            // Check for duplicate
            PCollection<KV<String, DeviceProfile>> deviceProf =pcList.get(0).apply("PCollectionList(deviceProfile)",
                    ParDo.of(new DoFn<String,KV<String, DeviceProfile>>() {
                @ProcessElement
                public void process(ProcessContext c) throws InvalidProtocolBufferException {
                    DeviceProfile dp = ProtoUtils.decodeMessageBase64(DeviceProfile.parser(), c.element());
                    if (DeviceProfile.getDefaultInstance().equals(dp)) {
                        return;
                    }
                    c.output(KV.of(dp.getDeviceId().getOs()+"$"+dp.getDeviceId().getUuid().toLowerCase(),dp));
                }
            }));
            deviceProf.apply(Count.perKey()).apply(
                Filter.by((ProcessFunction<KV<String, Long>, Boolean>) input2 -> {
                    if (input2.getValue() > 1L) {
                        throw new CorruptedDataException("More than 1 DeviceId found");
                    }
                    return true;
            }));

            PCollection<DeviceProfile> deviceProfile2 = deviceProf.apply(Values.create());
            PCollection<KV<String, Profile.InAppPurchaseProfile>> iapMap =
                    pcList.get(2).apply("PCollectionList(InAppPurchase)",ParDo.of(new DoFn<String, KV<String, Profile.InAppPurchaseProfile>>() {
                        @ProcessElement
                        public void process(ProcessContext c) throws InvalidProtocolBufferException {
                            Profile.InAppPurchaseProfile iap =
                                    ProtoUtils.decodeMessageBase64(Profile.InAppPurchaseProfile.parser(), c.element());
                            if (!Profile.InAppPurchaseProfile.getDefaultInstance().equals(iap)) {
                                c.output(KV.of(iap.getBundle(), iap));
                            }
                        }
                    }));

            PCollectionView<Map<String, Profile.InAppPurchaseProfile>> iapMapView =
                    iapMap.apply("IAPMapView", View.asMap());

            return deviceProfile2.apply("DeviceId,Map",ParDo.of(new DoFn<DeviceProfile, KV<DeviceId, float[]>>() {
                List<DeviceId> blackList;
                Map<String, Profile.InAppPurchaseProfile> map ;
                @ProcessElement
                public void process(ProcessContext c) {

                    try {
                        if (blackList == null) {
                            blackList = new ArrayList<>();
                            blackList.addAll(c.sideInput(blackListView));
                        }
                        if (c.element().getDeviceId().getUuid().charAt(7) == '0' && !blackList.contains(c.element().getDeviceId())) {
                            if (map == null) {
                                map = new HashMap<>(c.sideInput(iapMapView));
                            }
                            c.output(KV.of(c.element().getDeviceId(), PredictionUtils.getInputFeatures(c.element(), map)));
                        }

                    }
                    catch (CorruptedDataException e){
                        e.printStackTrace();
                    }
                }
            }).withSideInputs(iapMapView,blackListView));

        }
    }
}
