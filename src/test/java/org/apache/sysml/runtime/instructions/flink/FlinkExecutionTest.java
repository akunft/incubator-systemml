/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.sysml.runtime.instructions.flink;

import org.apache.sysml.api.DMLScript;
import org.junit.Test;

public class FlinkExecutionTest {
    String resourcePath = getClass().getClassLoader().getResource("flink").getPath();

    @Test
    public void TSMMTest() throws Exception {
        String[] args = {"-f",
                "/home/fschueler/Repos/incubator-systemml/scripts/myScripts/tsmm.dml",
                "-config=/home/fschueler/Repos/incubator-systemml/conf/SystemML-config.xml",
                "-exec",
                "hybrid_flink"};
        DMLScript.main(args);
    }

    @Test
    public void L2SVMTrainTest() throws Exception {
        String xFile = resourcePath + "/haberman.train.data.csv";
        String yFile = resourcePath + "/haberman.train.labels.csv";
        String model = resourcePath + "/l2-svm-model.csv";
        String log = resourcePath + "/l2-svm-log.csv";
        // Instructions (spark)
        // *
        // TSMM
        //TODO
        // Mapmm
        // r'
        // map+
        // map-

        String[] args = {
                "-f", "/home/fschueler/Repos/incubator-systemml/scripts/algorithms/l2-svm.dml",
                "-config=/home/fschueler/Repos/incubator-systemml/conf/SystemML-config.xml",
                "-exec", "hybrid_flink",
                "-nvargs",
                "X=" + xFile,
                "Y=" + yFile,
                "model=" + model,
                "fmt=\"csv\"",
                "Log=" + log};
        DMLScript.main(args);
    }

    @Test
    public void L2SVMEvaluateTest() throws Exception {
        String xFile = resourcePath + "/haberman.test.data.csv";
        String yFile = resourcePath + "/haberman.test.labels.csv";
        String model = resourcePath + "/l2-svm-model.csv";
        String conf = resourcePath + "/l2-svm-confusion.csv";

        String[] args = {
                "-f", "/home/fschueler/Repos/incubator-systemml/scripts/algorithms/l2-svm-predict.dml",
                "-config=/home/fschueler/Repos/incubator-systemml/conf/SystemML-config.xml",
                "-exec", "hybrid_flink",
                "-nvargs",
                "X=" + xFile,
                "Y=" + yFile,
                "model=" + model,
                "fmt=\"csv\"",
                "confusion=" + conf};
        DMLScript.main(args);
    }
}
