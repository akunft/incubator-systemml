package org.apache.sysml.runtime.instructions.flink.utils;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.sysml.runtime.matrix.MatrixCharacteristics;
import org.junit.Test;

import static org.junit.Assert.*;

public class BlockJoinUtilsTest {


    @Test
    public void testJoinThenBlock() throws Exception {
        String testFile = getClass().getClassLoader().getResource("flink/haberman.data").getFile();
        int numRowsPerBlock = 2;
        int numColsPerBlock = 2;
        MatrixCharacteristics mcOut = new MatrixCharacteristics(0L, 0L, numRowsPerBlock, numColsPerBlock);

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> i1 = env.readTextFile(testFile);
        DataSet<String> i2 = env.readTextFile(testFile);

        DataSet<String> joined = i1.join(i2).where(new KeySelector<String, Integer>() {
            @Override
            public Integer getKey(String s) throws Exception {
                return Integer.parseInt(s.split(",")[0]);
            }
        }).equalTo(new KeySelector<String, Integer>() {
            @Override
            public Integer getKey(String s) throws Exception {
                return Integer.parseInt(s.split(",")[0]);
            }
        }).with(new JoinFunction<String, String, String>() {
            @Override
            public String join(String a, String b) throws Exception {
                return a + b;
            }
        });
    }

    @Test
    public void testCsvBlockJoin() throws Exception {
        String testFile = getClass().getClassLoader().getResource("flink/haberman.data").getFile();
//        int numRowsPerBlock = 2;
//        int numColsPerBlock = 2;
//        MatrixCharacteristics mcOut = new MatrixCharacteristics(0L, 0L, numRowsPerBlock, numColsPerBlock);
//
//        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        DataSet<String> i1 = env.readTextFile(testFile);
//        DataSet<String> i2 = env.readTextFile(testFile);
//
//        DataSet<Tuple2<MatrixIndexes, MatrixBlock>> combined = DataSetConverterUtils.csvBlockJoin(env, i1, i2, mcOut, false, ",", false, 0.0);
//
//        List<Tuple2<MatrixIndexes, MatrixBlock>> blocks = combined.collect();
//
//        env = ExecutionEnvironment.getExecutionEnvironment();
//
//        DataSet<String> a1 = env.readTextFile(testFile);
//        DataSet<String> a2 = env.readTextFile(testFile);
//
//        mcOut = new MatrixCharacteristics(0L, 0L, numRowsPerBlock, numColsPerBlock);
//
//        DataSet<Tuple2<MatrixIndexes, MatrixBlock>> mat = DataSetConverterUtils.joinThenBlock(env, a1, a2, mcOut, false, ",", false, 0.0);
//
//        List<Tuple2<MatrixIndexes, MatrixBlock>> join = mat.collect();
//
//        System.out.println("done");
    }
}