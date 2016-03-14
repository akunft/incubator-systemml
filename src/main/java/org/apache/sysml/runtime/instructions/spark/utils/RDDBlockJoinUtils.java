package org.apache.sysml.runtime.instructions.spark.utils;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.Accumulator;
import org.apache.spark.AccumulatorParam;
import org.apache.spark.Accumulators;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.columnar.LONG;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.instructions.spark.data.SerLongWritable;
import org.apache.sysml.runtime.instructions.spark.data.SerText;
import org.apache.sysml.runtime.io.IOUtilFunctions;
import org.apache.sysml.runtime.matrix.MatrixCharacteristics;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.apache.sysml.runtime.util.UtilFunctions;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;

public class RDDBlockJoinUtils {

    public static JavaPairRDD<MatrixIndexes, MatrixBlock> csvToBlock(JavaSparkContext sc,
                                                                     JavaPairRDD<Integer, String> input, MatrixCharacteristics mcOut,
                                                                     boolean hasHeader, String delim, boolean fill, double fillValue)
            throws DMLRuntimeException {

        if (!mcOut.dimsKnown(true)) {
            Accumulator<Long> aNnz = sc.accumulator(0L, new LongAccu());
            JavaRDD<String> tmp = input.values().map(new Analyse(aNnz, delim));
            long rlen = tmp.count() - (hasHeader ? 1 : 0);
            long clen = tmp.first().split(delim).length;
            long nnz = UtilFunctions.toLong(aNnz.value());
            mcOut.set(rlen, clen, mcOut.getRowsPerBlock(), mcOut.getColsPerBlock(), nnz);
        }

//        JavaPairRDD<String, Long> prepinput = input.zipWithIndex(); //zip row index

        //convert csv rdd to binary block rdd (w/ partial blocks)
        JavaPairRDD<MatrixIndexes, MatrixBlock> out =
                input.mapPartitionsToPair(new TuplesToPair(mcOut, delim, fill, fillValue));

        out = RDDAggregateUtils.mergeByKey( out );

        return out;
    }

    public static JavaPairRDD<MatrixIndexes, MatrixBlock> blockJoin(JavaSparkContext sc,
                                                                    JavaRDD<String> i1,
                                                                    JavaRDD<String> i2,
                                                                    MatrixCharacteristics mcOut,
                                                                    boolean hasHeader,
                                                                    String delim,
                                                                    boolean fill,
                                                                    double fillValue) throws DMLRuntimeException {

        // build blocks for inputs
        JavaPairRDD<MatrixIndexes, MatrixBlock> x = getPartialAggregate(sc, i1, mcOut, hasHeader, delim, fill, fillValue, 0, true);
        int offset = (int) Math.ceil((double) mcOut.getCols() / mcOut.getColsPerBlock());
        JavaPairRDD<MatrixIndexes, MatrixBlock> y = getPartialAggregate(sc, i2, mcOut, hasHeader, delim, fill, fillValue, offset, false);

        JavaPairRDD<MatrixIndexes, MatrixBlock> out = x.cogroup(y).flatMapValues(new Function<Tuple2<Iterable<MatrixBlock>, Iterable<MatrixBlock>>, Iterable<MatrixBlock>>() {
            @Override
            public Iterable<MatrixBlock> call(Tuple2<Iterable<MatrixBlock>, Iterable<MatrixBlock>> tuple) throws Exception {
                ArrayList<MatrixBlock> out = new ArrayList<MatrixBlock>();

                Iterator<MatrixBlock> aItr = tuple._1().iterator();
                MatrixBlock value = null;
                if (aItr.hasNext()) {
                    value = aItr.next();
                    MatrixBlock b1 = new MatrixBlock(value);
                    long b1Nz = b1.getNonZeros();
                    while (aItr.hasNext()) {
                        value = aItr.next();
                        final MatrixBlock b2 = value;
                        long b2Nz = b2.getNonZeros();

//                        if (b1.getNumRows() != b2.getNumRows() || b1.getNumColumns() != b2.getNumColumns()) {
//                            throw new DMLRuntimeException("Mismatched block sizes for: "
//                                    + b1.getNumRows() + " " + b1.getNumColumns() + " "
//                                    + b2.getNumRows() + " " + b2.getNumColumns());
//                        }

                        // execute merge (never pass by reference)
                        b1.merge(b2, false);
                        b1.examSparsity();

//                        // sanity check output number of non-zeros
//                        if (b1.getNonZeros() != b1Nz + b2Nz) {
//                            throw new DMLRuntimeException("Number of non-zeros does not match: "
//                                    + b1.getNonZeros() + " != " + b1Nz + " + " + b2Nz);
//                        }
                    }
                    out.add(b1);
                }

                aItr = tuple._2().iterator();
                if (aItr.hasNext()) {
                    value = aItr.next();
                    MatrixBlock b1 = new MatrixBlock(value);
                    long b1Nz = b1.getNonZeros();
                    while (aItr.hasNext()) {
                        value = aItr.next();
                        final MatrixBlock b2 = value;
                        long b2Nz = b2.getNonZeros();

//                        if (b1.getNumRows() != b2.getNumRows() || b1.getNumColumns() != b2.getNumColumns()) {
//                            throw new DMLRuntimeException("Mismatched block sizes for: "
//                                    + b1.getNumRows() + " " + b1.getNumColumns() + " "
//                                    + b2.getNumRows() + " " + b2.getNumColumns());
//                        }

                        // execute merge (never pass by reference)
                        b1.merge(b2, false);
                        b1.examSparsity();

//                        // sanity check output number of non-zeros
//                        if (b1.getNonZeros() != b1Nz + b2Nz) {
//                            throw new DMLRuntimeException("Number of non-zeros does not match: "
//                                    + b1.getNonZeros() + " != " + b1Nz + " + " + b2Nz);
//                        }
                    }
                    out.add(b1);
                }
                return out;
            }
        });


        return out;
    }

    public static JavaPairRDD<MatrixIndexes, MatrixBlock> getPartialAggregate(JavaSparkContext sc,
                                                                              JavaRDD<String> input, MatrixCharacteristics mcOut,
                                                                              boolean hasHeader, String delim, boolean fill, double fillValue, int offset, boolean analyse)
            throws DMLRuntimeException {

        if (!mcOut.dimsKnown(true)) {
            Accumulator<Long> aNnz = sc.accumulator(0L, new LongAccu());
            JavaRDD<String> tmp = input.map(new Analyse(aNnz, delim));
            long rlen = tmp.count() - (hasHeader ? 1 : 0);
            long clen = tmp.first().split(delim).length - 1;
            long nnz = UtilFunctions.toLong(aNnz.value());
            mcOut.set(rlen, clen, mcOut.getRowsPerBlock(), mcOut.getColsPerBlock(), nnz);
        }

//        JavaPairRDD<String, Long> prepinput = input.zipWithIndex(); //zip row index

        //convert csv rdd to binary block rdd (w/ partial blocks)
        JavaPairRDD<MatrixIndexes, MatrixBlock> out =
                input.mapPartitionsToPair(new StringToBlock(mcOut, delim, fill, fillValue, offset));

        return out;
    }

    private static class StringToSerTextFunction implements PairFunction<String, LongWritable, Text> {
        private static final long serialVersionUID = 2286037080400222528L;

        @Override
        public scala.Tuple2<LongWritable, Text> call(String arg0)
                throws Exception {
            SerLongWritable slarg = new SerLongWritable(1L);
            SerText starg = new SerText(arg0);
            return new scala.Tuple2<LongWritable, Text>(slarg, starg);
        }
    }

    private static class StringToBlock
            implements PairFlatMapFunction<Iterator<String>, MatrixIndexes, MatrixBlock> {
        private static final long serialVersionUID = -4948430402942717043L;

        private long _rlen = -1;
        private long _clen = -1;
        private int _brlen = -1;
        private int _bclen = -1;
        private String _delim = null;
        private boolean _fill = false;
        private double _fillValue = 0;
        private int _offset = 0;

        public StringToBlock(MatrixCharacteristics mc, String delim, boolean fill, double fillValue, int offset) {
            _rlen = mc.getRows();
            _clen = mc.getCols();
            _brlen = mc.getRowsPerBlock();
            _bclen = mc.getColsPerBlock();
            _delim = delim;
            _fill = fill;
            _fillValue = fillValue;
            _offset = offset;
        }

        @Override
        public Iterable<scala.Tuple2<MatrixIndexes, MatrixBlock>> call(Iterator<String> arg0)
                throws Exception {
            ArrayList<scala.Tuple2<MatrixIndexes, MatrixBlock>> ret = new ArrayList<scala.Tuple2<MatrixIndexes, MatrixBlock>>();

            int ncblks = (int) Math.ceil((double) _clen / _bclen);
            MatrixIndexes[] ix = new MatrixIndexes[ncblks];
            MatrixBlock[] mb = new MatrixBlock[ncblks];

            while (arg0.hasNext()) {
                String row = arg0.next();
                String[] parts = IOUtilFunctions.split(row, _delim);
                long rowix = Integer.parseInt(parts[0]) + 1;

                long rix = UtilFunctions.computeBlockIndex(rowix, _brlen);
                int pos = UtilFunctions.computeCellInBlock(rowix, _brlen);

                //create new blocks for entire row
                if (ix[0] == null || ix[0].getRowIndex() != rix) {
                    if (ix[0] != null)
                        flushBlocksToList(ix, mb, ret);
                    long len = UtilFunctions.computeBlockSize(_rlen, rix, _brlen);
                    createBlocks(rowix, (int) len, ix, mb);
                }

                //process row data
                boolean emptyFound = false;
                for (int cix = 1, pix = 1; cix <= ncblks; cix++) {
                    int lclen = (int) UtilFunctions.computeBlockSize(_clen, cix, _bclen);
                    for (int j = 0; j < lclen; j++) {
                        String part = parts[pix++];
                        emptyFound |= part.isEmpty() && !_fill;
                        double val = (part.isEmpty() && _fill) ?
                                _fillValue : Double.parseDouble(part);
                        mb[cix - 1].appendValue(pos, j, val);
                    }
                }

                //sanity check empty cells filled w/ values
                IOUtilFunctions.checkAndRaiseErrorCSVEmptyField(row, _fill, emptyFound);
            }

            //flush last blocks
            flushBlocksToList(ix, mb, ret);

            return ret;
        }

        // Creates new state of empty column blocks for current global row index.
        private void createBlocks(long rowix, int lrlen, MatrixIndexes[] ix, MatrixBlock[] mb) {
            //compute row block index and number of column blocks
            long rix = UtilFunctions.computeBlockIndex(rowix, _brlen);
            int ncblks = (int) Math.ceil((double) _clen / _bclen);

            //create all column blocks (assume dense since csv is dense text format)
            for (int cix = 1; cix <= ncblks; cix++) {
                int lclen = (int) UtilFunctions.computeBlockSize(_clen, cix, _bclen);
                ix[cix - 1] = new MatrixIndexes(rix, cix + _offset);
                mb[cix - 1] = new MatrixBlock(lrlen, lclen, false);
            }
        }

        // Flushes current state of filled column blocks to output list.
        private void flushBlocksToList(MatrixIndexes[] ix, MatrixBlock[] mb, ArrayList<scala.Tuple2<MatrixIndexes, MatrixBlock>> ret)
                throws DMLRuntimeException {
            int len = ix.length;
            for (int i = 0; i < len; i++)
                if (mb[i] != null) {
                    ret.add(new scala.Tuple2<MatrixIndexes, MatrixBlock>(ix[i], mb[i]));
                    mb[i].examSparsity(); //ensure right representation
                }
        }
    }

    private static class TuplesToPair
            implements PairFlatMapFunction<Iterator<Tuple2<Integer, String>>, MatrixIndexes, MatrixBlock> {
        private static final long serialVersionUID = -4948430402942717043L;

        private long _rlen = -1;
        private long _clen = -1;
        private int _brlen = -1;
        private int _bclen = -1;
        private String _delim = null;
        private boolean _fill = false;
        private double _fillValue = 0;

        public TuplesToPair(MatrixCharacteristics mc, String delim, boolean fill, double fillValue) {
            _rlen = mc.getRows();
            _clen = mc.getCols();
            _brlen = mc.getRowsPerBlock();
            _bclen = mc.getColsPerBlock();
            _delim = delim;
            _fill = fill;
            _fillValue = fillValue;
        }

        @Override
        public Iterable<scala.Tuple2<MatrixIndexes, MatrixBlock>> call(Iterator<Tuple2<Integer, String>> arg0)
                throws Exception {
            ArrayList<scala.Tuple2<MatrixIndexes, MatrixBlock>> ret = new ArrayList<scala.Tuple2<MatrixIndexes, MatrixBlock>>();

            int ncblks = (int) Math.ceil((double) _clen / _bclen);
            MatrixIndexes[] ix = new MatrixIndexes[ncblks];
            MatrixBlock[] mb = new MatrixBlock[ncblks];

            while (arg0.hasNext()) {
                Tuple2<Integer, String> itr = arg0.next();
                String row = itr._2();
                String[] parts = IOUtilFunctions.split(row, _delim);
                long rowix = itr._1() + 1;

                long rix = UtilFunctions.computeBlockIndex(rowix, _brlen);
                int pos = UtilFunctions.computeCellInBlock(rowix, _brlen);

                //create new blocks for entire row
                if (ix[0] == null || ix[0].getRowIndex() != rix) {
                    if (ix[0] != null)
                        flushBlocksToList(ix, mb, ret);
                    long len = UtilFunctions.computeBlockSize(_rlen, rix, _brlen);
                    createBlocks(rowix, (int) len, ix, mb);
                }

                //process row data
                boolean emptyFound = false;
                for (int cix = 1, pix = 0; cix <= ncblks; cix++) {
                    int lclen = (int) UtilFunctions.computeBlockSize(_clen, cix, _bclen);
                    for (int j = 0; j < lclen; j++) {
                        String part = parts[pix++];
                        emptyFound |= part.isEmpty() && !_fill;
                        double val = (part.isEmpty() && _fill) ?
                                _fillValue : Double.parseDouble(part);
                        mb[cix - 1].appendValue(pos, j, val);
                    }
                }

                //sanity check empty cells filled w/ values
                IOUtilFunctions.checkAndRaiseErrorCSVEmptyField(row, _fill, emptyFound);
            }

            //flush last blocks
            flushBlocksToList(ix, mb, ret);

            return ret;
        }

        // Creates new state of empty column blocks for current global row index.
        private void createBlocks(long rowix, int lrlen, MatrixIndexes[] ix, MatrixBlock[] mb) {
            //compute row block index and number of column blocks
            long rix = UtilFunctions.computeBlockIndex(rowix, _brlen);
            int ncblks = (int) Math.ceil((double) _clen / _bclen);

            //create all column blocks (assume dense since csv is dense text format)
            for (int cix = 1; cix <= ncblks; cix++) {
                int lclen = (int) UtilFunctions.computeBlockSize(_clen, cix, _bclen);
                ix[cix - 1] = new MatrixIndexes(rix, cix);
                mb[cix - 1] = new MatrixBlock(lrlen, lclen, false);
            }
        }

        // Flushes current state of filled column blocks to output list.
        private void flushBlocksToList(MatrixIndexes[] ix, MatrixBlock[] mb, ArrayList<scala.Tuple2<MatrixIndexes, MatrixBlock>> ret)
                throws DMLRuntimeException {
            int len = ix.length;
            for (int i = 0; i < len; i++)
                if (mb[i] != null) {
                    ret.add(new scala.Tuple2<MatrixIndexes, MatrixBlock>(ix[i], mb[i]));
                    mb[i].examSparsity(); //ensure right representation
                }
        }
    }

    private static class Analyse implements Function<String, String> {
        private static final long serialVersionUID = 2310303223289674477L;

        private Accumulator<Long> _aNnz = null;
        private String _delim = null;

        public Analyse(Accumulator<Long> aNnz, String delim) {
            _aNnz = aNnz;
            _delim = delim;
        }

        @Override
        public String call(String line)
                throws Exception {
            //parse input line
            String[] cols = IOUtilFunctions.split(line, _delim);

            //determine number of non-zeros of row (w/o string parsing)
            long lnnz = 0;
            for (String col : cols) {
                if (!col.isEmpty() && !col.equals("0") && !col.equals("0.0")) {
                    lnnz++;
                }
            }

            //update counters
            _aNnz.add(lnnz);

            return line;
        }

    }

    private static class LongAccu implements AccumulatorParam<Long> {

        @Override
        public Long addAccumulator(Long t1, Long t2) {
            return addInPlace(t1, t2);
        }

        @Override
        public Long addInPlace(Long r1, Long r2) {
            return r1 + r2;
        }

        @Override
        public Long zero(Long initialValue) {
            return 0L;
        }
    }
}
