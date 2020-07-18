package edu.usfca.dataflow.utils;

import com.google.common.collect.Iterables;
import com.google.protobuf.InvalidProtocolBufferException;
import edu.usfca.dataflow.CorruptedDataException;
import edu.usfca.protobuf.Bid.Exchange;
import edu.usfca.protobuf.Common.DeviceId;
import edu.usfca.protobuf.Common.OsType;
import edu.usfca.protobuf.Profile.DeviceProfile;
import edu.usfca.protobuf.Profile.DeviceProfile.AppActivity;
import edu.usfca.protobuf.Profile.DeviceProfile.Builder;
import edu.usfca.protobuf.Profile.DeviceProfile.GeoActivity;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * This file was copied from reference solution of a previous project, and slightly modified for the purpose of this
 * project.
 * <p>
 * You can assume that all methods in this class are correct (once you fix a silly bug in some methods).
 * <p>
 * It is up to you whether you re-use this code or you write your own.
 * <p>
 * Also, if you are unsure about something or have questions, feel free to ask on Piazza (but also do check Project 2
 * instructions as you may find what you need faster that way).
 * <p>
 * It's recommended that you use the provided code to start with, until you pass all unit tests, and then begin
 * optimizing your code. That way, you will have a correct implementation to refer to, as things may go south while
 * optimizing things.
 */
public class DeviceProfileUtils {

    /**
     * This method takes DeviceId proto, and lowers its uuid field (to normalize).
     * <p>
     * That way, you can easily compare two DeviceIds.
     */
    public static DeviceId getCanonicalId(DeviceId id) {
        return id.toBuilder().setUuid(id.getUuid().toLowerCase()).build();
    }

    /**
     * This returns true if the given DeviceId is valid.
     * <p>
     * In this project: DeviceId is considered valid if its Os is known (either IOS and Android) and its uuid is NOT
     * blank.
     * <p>
     * (This is the same method from Project 2.)
     * <p>
     * TODO: Fix a small bug in this!
     */
    public static boolean isDeviceIdValid(DeviceId did) {
        if (did.getOs() != OsType.ANDROID && did.getOs() != OsType.IOS) {
            return false;
        }

        return !StringUtils.isBlank(did.getUuid());
    }

    /**
     * (Note: You can assume that this method is correct; it's copied from Project 2 with minor changes.)
     * <p>
     * However, you do not have to use this method.
     * <p>
     * DeviceProfile is considered valid, if ALL of the following conditions are met:
     * <p>
     * (1) DeviceId is valid (according to the isDeviceIdValid(DeviceId) method above).
     * <p>
     * (2) 0 < first_at <= last_at.
     * <p>
     * (3) Each "AppActivity" proto in app field, all of the following must be met:
     * <p>
     * (3-1) DeviceProfile.first_at <= AppActivity.first_at <= AppActivity.last_at <= DeviceProfile.last_at
     * <p>
     * (3-2) AppActivity.bundle is not blank (use {@link org.apache.commons.lang3.StringUtils#isBlank(CharSequence)}
     * <p>
     * (3-3) Each value in count_per_exchange is positive
     * <p>
     * (3-4) Each key in count_per_exchange is a valid "enum value" of Exchange enum (defined in bid.proto). Note that
     * "UNKNOWN_EXCHANGE" has a valid enum value (0). You can check "bid.proto" to see which values are valid.
     * <p>
     * (3-5) Bundles are distinct.
     * <p>
     * (3-6) For at least one AppActivity proto, DeviceProfile.first_at = AppActivity.first_at.
     * <p>
     * (3-7) For at least one AppActivity proto, DeviceProfile.last_at = AppActivity.last_at.
     * <p>
     * (3-8) count_per_exchange must contain at least one entry.
     * <p>
     * (4) and (5) are omitted.
     * <p>
     * (6) Each "GeoActivity" proto in geo field, all of the following must be met:
     * <p>
     * (6-1) Neither country nor region is blank.
     * <p>
     * (6-2) For any two "GeoActivity" protos stored in "app" field, they are not the same (that is, their country and/or
     * region values must differ).
     * <p>
     * See sample unit tests for examples. If you do not implement this method correctly, other parts of your pipeline may
     * also fail.
     */
    public static boolean isDpValid(DeviceProfile dp) {
        // (1) Check DeviceId
        if (!isDeviceIdValid(dp.getDeviceId())) {
            return false;
        }
        if (!(0 < dp.getFirstAt() && dp.getFirstAt() <= dp.getLastAt())) {
            // (2)
            return false;
        }
        Set<String> bundles = new HashSet<>();
        long appFirst = Long.MAX_VALUE, appLast = Long.MIN_VALUE;
        for (AppActivity app : dp.getAppList()) {
            if (StringUtils.isBlank(app.getBundle())) {
                // (3-2)
                return false;
            }
            bundles.add(app.getBundle());
            if (!(dp.getFirstAt() <= app.getFirstAt() && app.getFirstAt() <= app.getLastAt()
                    && app.getLastAt() <= dp.getLastAt())) {
                // (3-1)
                return false;
            }
            if (app.getCountPerExchangeCount() < 1) {
                // (3-8)
                return false;
            }
            for (Entry<Integer, Integer> et : app.getCountPerExchangeMap().entrySet()) {
                if (et.getKey() < 0 || Exchange.forNumber(et.getKey()) == null) {
                    // (3-4)
                    return false;
                }
                if (et.getValue() <= 0) {
                    // (3-3)
                    return false;
                }
            }
            appFirst = Math.min(appFirst, app.getFirstAt());
            appLast = Math.max(appLast, app.getLastAt());
        }
        if (appFirst != dp.getFirstAt() || appLast != dp.getLastAt()) {
            // (3-6) & (3-7)
            return false;
        }
        if (bundles.size() != dp.getAppCount()) {
            // (3-5)
            return false;
        }
        // For this project, this conditions are no longer valid.
        // if (StringUtils.isBlank(dp.getLatestGeo().getCountry()) || StringUtils.isBlank(dp.getLatestGeo().getRegion())) {
        // // (4) and (5)
        // return false;
        // }
        Set<KV<String, String>> geos = new HashSet<>();
        for (GeoActivity geo : dp.getGeoList()) {
            if (StringUtils.isBlank(geo.getCountry()) || StringUtils.isBlank(geo.getRegion())) {
                // (6-1)
                return false;
            }
            geos.add(KV.of(geo.getCountry(), geo.getRegion()));
        }
        // (6-2)
        return geos.size() == dp.getGeoCount();
        // For this project, this condition is no longer valid.
        // if (!geos.contains(KV.of(dp.getLatestGeo().getCountry(), dp.getLatestGeo().getRegion()))) {
        // // (6-3)
        // return false;
        // }
    }

    /**
     * You can assume that this method is correct (copied from project 2 reference solution).
     * <p>
     * However, you are welcome to modify it or write your own (as long as the unit tests do not break).
     * <p>
     * Given two AppActivity protos, return the merged AppActivity proto.
     */
    public static AppActivity mergeApps(AppActivity app1, AppActivity app2) {
        if (app1 == null) {
            return app2;
        }
        AppActivity.Builder merged = app2.toBuilder();
        merged.setFirstAt(Math.min(merged.getFirstAt(), app1.getFirstAt()));
        merged.setLastAt(Math.max(merged.getLastAt(), app1.getLastAt()));
        for (Entry<Integer, Integer> et : app1.getCountPerExchangeMap().entrySet()) {
            merged.putCountPerExchange(et.getKey(), merged.getCountPerExchangeOrDefault(et.getKey(), 0) + et.getValue());
        }
        return merged.build();
    }

    /**
     * TODO: Finish implementing this, if you intend to use this.
     * <p>
     * (You do not have to! You can reuse your old code from project 2 or instructor's code for project 2.
     * <p>
     * Or you can write everything from scratch.)
     */
    public static DeviceProfile mergeDps(Iterable<DeviceProfile> dps) {

        if (Iterables.size(dps) == 0) {
            return DeviceProfile.getDefaultInstance();
        }
        if (Iterables.size(dps) == 1) {
            return dps.iterator().next();
        }
        Builder merged = null;
        Map<KV<String, String>, Long> geos = new HashMap<>();
        Map<String, AppActivity> apps = new HashMap<>();
        for (DeviceProfile dp : dps) {
            for (GeoActivity geo : dp.getGeoList()) {
                KV<String, String> kv = KV.of(geo.getCountry(), geo.getRegion());
                geos.put(kv, (long) 0);
            }
            // Update apps with latest info.
            for (AppActivity app : dp.getAppList()) {
                apps.put(app.getBundle(), mergeApps(apps.getOrDefault(app.getBundle(), null), app));
            }
            if (merged == null) {
                merged = dp.toBuilder().clearGeo().clearApp();
                continue;
            }
            merged.setFirstAt(Math.min(merged.getFirstAt(), dp.getFirstAt()));
            merged.setLastAt(Math.max(merged.getLastAt(), dp.getLastAt()));
        }

        for (Entry<KV<String, String>, Long> et : geos.entrySet()) {
            merged.addGeo(GeoActivity.newBuilder().setCountry(et.getKey().getKey()).setRegion(et.getKey().getValue()).build());
        }
        merged.addAllApp(apps.values());

        return merged.build();
    }

    public static class GetDeviceId extends DoFn<DeviceProfile, KV<String, DeviceProfile>> {
        @ProcessElement
        public void process(@Element DeviceProfile dp, OutputReceiver<KV<String, DeviceProfile>> out) {
            if (!DeviceProfileUtils.isDpValid(dp)) {
                throw new CorruptedDataException("Invalid DeviceProfile was found");
            }
            out.output(KV.of(dp.getDeviceId().getOs()+"$"+dp.getDeviceId().getUuid(),
                    dp));
        }
    }

}
