package org.apache.sysml.runtime.instructions.spark.utils;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.random.RandomRDDs;
import org.apache.spark.mllib.random.UniformGenerator;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.matrix.MatrixCharacteristics;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.Tuple2;

import java.io.File;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class RDDBlockJoinUtilsTest {

    private static final int numTasks = 1;
    private static final int features = 4;
    private static final long tuplesPerTask = 1000;
    private static final long SEED = 12345;
    private static final int lowerBound = 0;
    private static final int upperBound = 100;
    public static final String TMP_GEN_FK = "/tmp/gen/fk";
    public static final String TMP_GEN_PK = "/tmp/gen/pk";

    public static void deleteFolder(File folder) {
        File[] files = folder.listFiles();
        if(files!=null) { //some JVMs return null for empty dirs
            for(File f: files) {
                if(f.isDirectory()) {
                    deleteFolder(f);
                } else {
                    f.delete();
                }
            }
        }
        folder.delete();
    }

    @BeforeClass
    public static void beforeTest() throws Exception {
        deleteFolder(new File("/tmp/gen"));

        SparkConf conf = new SparkConf().setAppName("generateData").setMaster("local[" + numTasks + "]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        long N = tuplesPerTask * numTasks; // number of points generated in total

        final UniformGenerator aggValuesGenerator = new UniformGenerator();
        aggValuesGenerator.setSeed(SEED);

//        JavaRDD<String> fkTable = RandomRDDs.uniformJavaRDD(sc, N, numTasks, SEED).map(new Function<Double, String>() {
//            @Override
//            public String call(Double v1) throws Exception {
//                int lower = lowerBound;
//                int upper = upperBound;
//
//                int key = (int) (lower + (upper - lower) * v1);
//                StringBuilder builder = new StringBuilder();
//                for (int i = 0; i < features; i++) {
//                    builder.append(aggValuesGenerator.nextValue());
//                    builder.append(",");
//                }
//                return key + "," + builder.toString().substring(0, builder.length() - 1);
//            }
//        });
//        fkTable.saveAsTextFile(TMP_GEN_FK);

        JavaRDD<String> pkTable = RandomRDDs.normalJavaRDD(sc, N/2).map(new Function<Double, String>() {
            @Override
            public String call(Double v1) throws Exception {
                StringBuilder builder = new StringBuilder();
                for (int i = 0; i < features; i++) {
                    builder.append(aggValuesGenerator.nextValue());
                    builder.append(",");
                }
                return builder.toString().substring(0, builder.length() - 1);
            }
        });
        pkTable.zipWithIndex().map(new Function<Tuple2<String,Long>, String>() {
            @Override
            public String call(Tuple2<String, Long> v1) throws Exception {
                return v1._2() + "," + v1._1();
            }
        }).saveAsTextFile(TMP_GEN_PK);

        pkTable.zipWithIndex().map(new Function<Tuple2<String,Long>, String>() {
            @Override
            public String call(Tuple2<String, Long> v1) throws Exception {
                return v1._2() + "," + v1._1();
            }
        }).saveAsTextFile(TMP_GEN_FK);

        sc.close();
    }

    @Test
    public void testJoinThenBlock() throws Exception {
        int numRowsPerBlock = 2;
        int numColsPerBlock = 2;
        MatrixCharacteristics mcOut = new MatrixCharacteristics(0L, 0L, numRowsPerBlock, numColsPerBlock);

        SparkConf conf = new SparkConf().setAppName("join-then-block").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // read
        JavaPairRDD<Integer, String> pk = sc.textFile(TMP_GEN_PK + "/part-00000").mapToPair(new KeySelector());
        JavaPairRDD<Integer, String> fk = sc.textFile(TMP_GEN_FK + "/part-00000").mapToPair(new KeySelector());
        // join
        JavaRDD<String> joined = pk.join(fk).map(new Concatenate());

        JavaPairRDD<MatrixIndexes, MatrixBlock> out = RDDConverterUtils.csvToBinaryBlock(sc, joined, mcOut, false, ",", false, 0.0);

        List<Tuple2<MatrixIndexes, MatrixBlock>> blocks = out.collect();
        sc.stop();

        assertEquals(150,blocks.size());
        assertEquals(11, mcOut.getCols());
        assertEquals(50, mcOut.getRows());
        assertEquals(560, mcOut.getNonZeros());
    }

    @Test
    public void testCsvBlockJoin() throws Exception {
        int numRowsPerBlock = 2;
        int numColsPerBlock = 2;
        MatrixCharacteristics mcOut = new MatrixCharacteristics(0L, 0L, numRowsPerBlock, numColsPerBlock);

        SparkConf conf = new SparkConf().setAppName("join-then-block").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // read
        JavaRDD<String> pk = sc.textFile(TMP_GEN_PK + "/part-00000");//.mapToPair(new KeySelector());
        JavaRDD<String> fk = sc.textFile(TMP_GEN_FK + "/part-00000");//.mapToPair(new KeySelector());

        JavaPairRDD<MatrixIndexes, MatrixBlock> out = RDDBlockJoinUtils.csvBlockJoin(sc, pk, fk, mcOut, false, ",", false, 0.0);
        List<Tuple2<MatrixIndexes, MatrixBlock>> blocks = out.collect();
        sc.stop();

        assertEquals(150,blocks.size());
        assertEquals(11, mcOut.getCols());
        assertEquals(50, mcOut.getRows());
        assertEquals(560, mcOut.getNonZeros());
    }

    @Test
    public void testAgainst() throws DMLRuntimeException {
        List<Tuple2<MatrixIndexes, MatrixBlock>> blockJoin = blockJoin();
        List<Tuple2<MatrixIndexes, MatrixBlock>> joinThenBlock = joinThenBlock();
        blockJoin.sort(new MatrixComparator());
        joinThenBlock.sort(new MatrixComparator());

//        assertEquals(blockJoin, joinThenBlock);

    }

    private static class MatrixComparator implements Comparator<Tuple2<MatrixIndexes, MatrixBlock>> {
        @Override
        public int compare(Tuple2<MatrixIndexes, MatrixBlock> a, Tuple2<MatrixIndexes, MatrixBlock> b) {
            return a._1().compareWithOrder(b._1(), false);
        }
    }

    public static List<Tuple2<MatrixIndexes, MatrixBlock>> blockJoin() throws DMLRuntimeException {
        int numRowsPerBlock = 2;
        int numColsPerBlock = 2;
        MatrixCharacteristics mcOut = new MatrixCharacteristics(0L, 0L, numRowsPerBlock, numColsPerBlock);

        long start = System.nanoTime();
        SparkConf conf = new SparkConf().setAppName("join-then-block").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // read
        JavaRDD<String> pk = sc.textFile(TMP_GEN_PK + "/part-00000").map(new RemoveKey());
        JavaRDD<String> fk = sc.textFile(TMP_GEN_FK + "/part-00000").map(new RemoveKey());

        JavaPairRDD<MatrixIndexes, MatrixBlock> out = RDDBlockJoinUtils.csvBlockJoin(sc, pk, fk, mcOut, false, ",", false, 0.0);
        List<Tuple2<MatrixIndexes, MatrixBlock>> blocks = out.collect();
        sc.stop();
        long stop = System.nanoTime();
        long time = (stop - start) / 1000000;
        System.out.println("blockjoin: " + time);

        return blocks;
    }

    public static List<Tuple2<MatrixIndexes, MatrixBlock>> joinThenBlock() throws DMLRuntimeException {
        int numRowsPerBlock = 2;
        int numColsPerBlock = 2;
        MatrixCharacteristics mcOut = new MatrixCharacteristics(0L, 0L, numRowsPerBlock, numColsPerBlock);
        long start = System.nanoTime();
        SparkConf conf = new SparkConf().setAppName("join-then-block").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // read
        JavaPairRDD<Integer, String> pk = sc.textFile(TMP_GEN_PK + "/part-00000").mapToPair(new KeySelector());
        JavaPairRDD<Integer, String> fk = sc.textFile(TMP_GEN_FK + "/part-00000").mapToPair(new KeySelector());
        // join
        JavaRDD<String> joined = pk.join(fk).map(new Concatenate());

        JavaPairRDD<MatrixIndexes, MatrixBlock> out = RDDConverterUtils.csvToBinaryBlock(sc, joined, mcOut, false, ",", false, 0.0);

        List<Tuple2<MatrixIndexes, MatrixBlock>> blocks = out.collect();
        sc.stop();
        long stop = System.nanoTime();
        long time = (stop - start) / 1000000;
        System.out.println("joinThenBlock: " + time);

        return blocks;
    }

    static class Concatenate implements Function<Tuple2<Integer,Tuple2<String,String>>, String> {
        @Override
        public String call(Tuple2<Integer, Tuple2<String, String>> in) throws Exception {
            return in._2()._1() + "," + in._2()._2();
        }
    }

    static class KeySelector implements PairFunction<String, Integer, String> {
        @Override
        public Tuple2<Integer, String> call(String s) throws Exception {
            String keyStr = s.split(",")[0];
            return new Tuple2<Integer, String>(Integer.parseInt(keyStr), s.substring(keyStr.length() + 1));
        }
    }

    static class RemoveKey implements Function<String, String> {
        @Override
        public String call(String s) throws Exception {
            String keyStr = s.split(",")[0];
            return s.substring(keyStr.length() + 1);
        }
    }
}