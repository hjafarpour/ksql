/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.function.udf.ml;

import hex.genmodel.GenModel;
import hex.genmodel.easy.EasyPredictModelWrapper;
import hex.genmodel.easy.RowData;
import hex.genmodel.easy.prediction.AutoEncoderModelPrediction;
import io.confluent.ksql.function.KsqlFunctionException;
import io.confluent.ksql.function.udf.Kudf;


public class AnomalyKudf implements Kudf {

  // Model built with H2O R API:
  // anomaly_model <- h2o.deeplearning(x = names(train_ecg),training_frame =
  // train_ecg,activation = "Tanh",autoencoder = TRUE,hidden =
  // c(50,20,50),sparse = TRUE,l1 = 1e-4,epochs = 100)

  // Name of the generated H2O model
  private static String modelClassName = "io.confluent.ksql.function.udf.ml"
                                         + ".DeepLearning_model_R_1509973865970_1";


  @Override
  public void init() {

  }

  @Override
  public Object evaluate(Object... args) {
    if (args.length != 1) {
      throw new KsqlFunctionException("Anomaly udf should have one input argument.");
    }
    try {
      return applyAnalyticModel(args[0]);

    } catch (Exception e) {
      throw new KsqlFunctionException("Model Inference failed. Please check the logs.");
    }
  }

  private Object applyAnalyticModel(Object object) throws Exception {

    // Gets here... => Query returns 'Hello Kai'
    // if (true) return "Hello Kai";

    GenModel rawModel;
    rawModel = (hex.genmodel.GenModel) Class.forName(modelClassName).newInstance();
    EasyPredictModelWrapper model = new EasyPredictModelWrapper(rawModel);


    double[] input = new double[] { 2.10, 2.13, 2.19, 2.28, 2.44, 2.62, 2.80, 3.04, 3.36, 3.69, 3.97, 4.24, 4.53,
                                    4.80, 5.02, 5.21, 5.40, 5.57, 5.71, 5.79, 5.86, 5.92, 5.98, 6.02, 6.06, 6.08, 6.14, 6.18, 6.22, 6.27,
                                    6.32, 6.35, 6.38, 6.45, 6.49, 6.53, 6.57, 6.64, 6.70, 6.73, 6.78, 6.83, 6.88, 6.92, 6.94, 6.98, 7.01,
                                    7.03, 7.05, 7.06, 7.07, 7.08, 7.06, 7.04, 7.03, 6.99, 6.94, 6.88, 6.83, 6.77, 6.69, 6.60, 6.53, 6.45,
                                    6.36, 6.27, 6.19, 6.11, 6.03, 5.94, 5.88, 5.81, 5.75, 5.68, 5.62, 5.61, 5.54, 5.49, 5.45, 5.42, 5.38,
                                    5.34, 5.31, 5.30, 5.29, 5.26, 5.23, 5.23, 5.22, 5.20, 5.19, 5.18, 5.19, 5.17, 5.15, 5.14, 5.17, 5.16,
                                    5.15, 5.15, 5.15, 5.14, 5.14, 5.14, 5.15, 5.14, 5.14, 5.13, 5.15, 5.15, 5.15, 5.14, 5.16, 5.15, 5.15,
                                    5.14, 5.14, 5.15, 5.15, 5.14, 5.13, 5.14, 5.14, 5.11, 5.12, 5.12, 5.12, 5.09, 5.09, 5.09, 5.10, 5.08,
                                    5.08, 5.08, 5.08, 5.06, 5.05, 5.06, 5.07, 5.05, 5.03, 5.03, 5.04, 5.03, 5.01, 5.01, 5.02, 5.01, 5.01,
                                    5.00, 5.00, 5.02, 5.01, 4.98, 5.00, 5.00, 5.00, 4.99, 5.00, 5.01, 5.02, 5.01, 5.03, 5.03, 5.02, 5.02,
                                    5.04, 5.04, 5.04, 5.02, 5.02, 5.01, 4.99, 4.98, 4.96, 4.96, 4.96, 4.94, 4.93, 4.93, 4.93, 4.93, 4.93,
                                    5.02, 5.27, 5.80, 5.94, 5.58, 5.39, 5.32, 5.25, 5.21, 5.13, 4.97, 4.71, 4.39, 4.05, 3.69, 3.32, 3.05,
                                    2.99, 2.74, 2.61, 2.47, 2.35, 2.26, 2.20, 2.15, 2.10, 2.08 };

    RowData row = new RowData();
    int j = 0;
    for (String colName : rawModel.getNames()) {

      row.put(colName, input[j]);
      j++;
    }

    AutoEncoderModelPrediction p = model.predictAutoEncoder(row);
    System.out.println("original: " + java.util.Arrays.toString(p.original));
    System.out.println("reconstructedrowData: " + p.reconstructedRowData);
    System.out.println("reconstructed: " + java.util.Arrays.toString(p.reconstructed));

    double sum = 0;
    for (int i = 0; i < p.original.length; i++) {
      sum += (p.original[i] - p.reconstructed[i]) * (p.original[i] - p.reconstructed[i]);
    }
    double mse = sum / p.original.length;
    System.out.println("MSE: " + mse);


    return ("MSE: " + mse);
  }
}
