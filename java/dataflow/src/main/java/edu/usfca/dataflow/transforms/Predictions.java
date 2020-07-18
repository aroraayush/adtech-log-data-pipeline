package edu.usfca.dataflow.transforms;

import edu.usfca.protobuf.Common.DeviceId;
import edu.usfca.protobuf.Data.PredictionData;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Instant;
import org.tensorflow.SavedModelBundle;
import org.tensorflow.Tensor;

import java.nio.FloatBuffer;
import java.util.*;

public class Predictions {

    /**
     * This method will be called by the unit tests.
     * <p>
     * The reason for having this method (instead of instantiating a specific DoFn) is to allow you to easily experiment
     * with different implementations of PredictDoFn.
     * <p>
     * The provided code (see below) for "PredictDoFnNever" is "correct" but extremely inefficient.
     * <p>
     * Use it as a reference to implement "PredictDoFn" instead.
     * <p>
     * When you are ready to optimize it, you'll find the ungraded homework for Lab 09 useful (as well as sample code from
     * L34: DF-TF).
     */
    public static DoFn<KV<DeviceId, float[]>, PredictionData> getPredictDoFn(String pathToModelDir) {
//    return new PredictDoFnNever(pathToModelDir);
        return new PredictDoFn(pathToModelDir);
    }

    // This utility method simply returns the index with largest prediction score.
    // Input must be an array of length 10.
    // This is provided for you (see PredictDoFnNever" to understand how it's used).
    static int getArgMax(float[] pred) {
        int prediction = -1;
        for (int j = 0; j < 10; j++) {
            if (prediction == -1 || pred[prediction] < pred[j]) {
                prediction = j;
            }
        }
        return prediction;
    }

    /**
     * Given (DeviceId, float[]) pairs, this DoFn will return (for each element) its prediction & score.
     * <p>
     * Prediction must be between 0 and 9 (inclusive), which "classifies" the input.
     * <p>
     * Score is a numerical value that quantifies the model's confidence.
     * <p>
     * The model returns 10 values (10 scores), and you should use the provided "getArgMax" method to obtain its
     * classification and score.
     * <p>
     * As final output, PredictionData (proto) should be returned, which has two fields (DeviceId and double).
     * <p>
     * NOTE: It's strongly recommended that you not change this code (so you can "keep" it as reference),
     * <p>
     * and instead start implementing your own in PredictDoFn below. Then, once you're ready, simply change
     * "getPredictDoFn" above to return an instance of your new DoFn.
     */
    static class PredictDoFnNever extends DoFn<KV<DeviceId, float[]>, PredictionData> {
        final static String tfTag = "serve"; // <- Do not change this.
        final String pathToModelDir;

        transient Tensor rate;
        transient SavedModelBundle mlBundle;

        public PredictDoFnNever(String pathToModelDir) {
            this.pathToModelDir = pathToModelDir;
        }

        // This method is provided for your convenience. Use it as a reference.
        // "inputFeatures" (float[][]) is assumed to be of size "1" by "768".
        // Note: Tensor<> objects are resources that must be explicitly closed to prevent memory leaks.
        // That's why you are seeing the try blocks below (with that, those resources are auto-closed).
        float[][] getPrediction(float[][] inputFeatures, SavedModelBundle mlBundle) {
            // "prediction" array will store the scores returned by the model (recall that the model returns 10 scores per
            // input).
            float[][] prediction = new float[1][10];
            try (Tensor<?> x = Tensor.create(inputFeatures)) {
                try (Tensor<?> output = mlBundle.session().runner().feed("input_tensor", x).feed("dropout/keep_prob", rate)
                        .fetch("output_tensor").run().get(0)) {
                    output.copyTo(prediction);
                }
            }
            return prediction;
        }

        @ProcessElement
        public void process(ProcessContext c) {
            // --------------------------------------------------------------------------------
            // Loading the model: TODO (if mlBundle == null) // move all below to setup/startbundle
            mlBundle = SavedModelBundle.load(pathToModelDir, tfTag);

            // This is necessary because the model expects to be given this additional tensor.
            // (For those who're familiar with NNs, keep_prob is 1 - dropout.)
            float[] keep_prob_arr = new float[1024];
            Arrays.fill(keep_prob_arr, 1f);
            rate = Tensor.create(new long[]{1, 1024}, FloatBuffer.wrap(keep_prob_arr));

            // --------------------------------------------------------------------------------

            // Prepare the input data (to be fed to the model).
            final DeviceId id = c.element().getKey();
            final float[][] inputData = new float[][]{c.element().getValue()};

            // Obtain the prediction scores from the model, and Find the index with maximum score (ties broken by favoring
            // smaller index).
            float[][] pred = getPrediction(inputData, mlBundle);
            int prediction = getArgMax(pred[0]);

            // Build PredictionData proto and output it.
            PredictionData.Builder pd =
                    PredictionData.newBuilder().setId(id).setPrediction(prediction).setScore(pred[0][prediction]);
            c.output(pd.build());
        }
    }

    /**
     * TODO: Use this starter code to implement your own PredictDoFn.
     * <p>
     * You'll need to utilize DoFn's annotated methods & optimization techniques that we discussed in L10, L30, L34, and
     * Lab09.
     */
    static class PredictDoFn extends DoFn<KV<DeviceId, float[]>, PredictionData> {
        final static String tfTag = "serve"; // <- Do not change this.
        final String pathToModelDir;
        final static int BUFFER_MAX_SIZE = 60;

        transient Tensor rate;
        transient SavedModelBundle mlBundle;
        List<KV<DeviceId,float[]>> buffer;

        public PredictDoFn(String pathToModelDir) {
            this.pathToModelDir = pathToModelDir;
        }

        // This method is provided for your convenience. Use it as a reference.
        // "inputFeatures" (float[][]) is assumed to be of size "1" by "768".
        // Note: Tensor<> objects are resources that must be explicitly closed to prevent memory leaks.
        // That's why you are seeing the try blocks below (with that, those resources are auto-closed).
        float[][] getPrediction(float[][] inputFeatures, SavedModelBundle mlBundle) {
        // "prediction" array will store the scores returned by the model (recall that the model returns 10 scores per
        // input).

          float[][] prediction = new float[inputFeatures.length][10];
          try (Tensor<?> x = Tensor.create(inputFeatures)) {
            try (Tensor<?> output = mlBundle.session().runner().feed("input_tensor", x).feed("dropout/keep_prob", rate)
                .fetch("output_tensor").run().get(0)) {
              output.copyTo(prediction);
            }
          }
          return prediction;
        }

        @Setup
        public void setup() {
            mlBundle = SavedModelBundle.load(pathToModelDir, tfTag);
            float[] keep_prob_arr = new float[1024];
            Arrays.fill(keep_prob_arr, 1f);
            rate = Tensor.create(new long[]{1, 1024}, FloatBuffer.wrap(keep_prob_arr));
            // This is necessary because the model expects to be given this additional tensor.
            // (For those who're familiar with NNs, keep_prob is 1 - dropout.)

        }

        @StartBundle
        public void startBundle() {
            buffer = new ArrayList<>();
        }

        @ProcessElement
        public void process(ProcessContext c) {
            // TODO: Implement this!
            buffer.add(KV.of(c.element().getKey(),c.element().getValue()));
            if (buffer.size() >= BUFFER_MAX_SIZE) {
                flush(c);
            }
        }

        // This will be called exactly once per bundle, after the last call to @process.
        @FinishBundle
        public void finishBundle(FinishBundleContext c) {
            // Prepare the input data (to be fed to the model).
            int size = buffer.size();
            if(size == 0){
                return;
            }
            float[][] inputData = new float[size][];
            for(int i = 0; i < size ; i++){
                inputData[i] = buffer.get(i).getValue();
            }
            float[][] pred = getPrediction(inputData, mlBundle);
            for(int i = 0; i < size ; i++) {
                int prediction = getArgMax(pred[i]);

                // Build PredictionData proto and output it.
                c.output(edu.usfca.protobuf.Data.PredictionData.newBuilder().setId(buffer.get(i).getKey())
                        .setPrediction(prediction).setScore(pred[i][prediction]).build(),
                        Instant.EPOCH, GlobalWindow.INSTANCE);
            }
        }

        public void flush(ProcessContext c) {

                // Prepare the input data (to be fed to the model).
                float[][] inputData = new float[buffer.size()][];
                for(int i = 0; i < buffer.size() ; i++){
                    inputData[i] = buffer.get(i).getValue();
                }
                float[][] pred = getPrediction(inputData, mlBundle);
                for(int i = 0; i < buffer.size() ; i++) {
                    int prediction = getArgMax(pred[i]);
                    // Build PredictionData proto and output it.
                    c.output(edu.usfca.protobuf.Data.PredictionData.newBuilder().setId(buffer.get(i).getKey())
                                    .setPrediction(prediction).setScore(pred[i][prediction]).build());
                }

            buffer.clear();
        }
    }
}
