package org.apache.sysml.runtime.instructions.flink.utils;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.io.IOUtilFunctions;
import org.apache.sysml.runtime.matrix.MatrixCharacteristics;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.apache.sysml.runtime.util.UtilFunctions;

import java.util.Iterator;
import java.util.List;

public class BlockJoinUtils {
    public static DataSet<Tuple2<MatrixIndexes, MatrixBlock>> csvBlockJoin(ExecutionEnvironment env,
                                                                           DataSet<String> i1,
                                                                           DataSet<String> i2,
                                                                           MatrixCharacteristics mcOut,
                                                                           boolean hasHeader,
                                                                           String delim,
                                                                           boolean fill,
                                                                           double fillValue) throws DMLRuntimeException {

        DataSet<Tuple2<MatrixIndexes, MatrixBlock>> x = getPartialAggregates(env, i1, mcOut, hasHeader, delim, fill, fillValue, 0);
        int offset = (int) Math.ceil((double) mcOut.getCols() / mcOut.getColsPerBlock());
        DataSet<Tuple2<MatrixIndexes, MatrixBlock>> y = getPartialAggregates(env, i2, mcOut, hasHeader, delim, fill, fillValue, offset);

        DataSet<Tuple2<MatrixIndexes, MatrixBlock>> out = x.coGroup(y)
                .where(0)
                .equalTo(0)
                .with(new CoGroupFunction<Tuple2<MatrixIndexes, MatrixBlock>, Tuple2<MatrixIndexes, MatrixBlock>, Tuple2<MatrixIndexes, MatrixBlock>>() {
                    @Override
                    public void coGroup(Iterable<Tuple2<MatrixIndexes, MatrixBlock>> a,
                                        Iterable<Tuple2<MatrixIndexes, MatrixBlock>> b,
                                        Collector<Tuple2<MatrixIndexes, MatrixBlock>> collector) throws Exception {

                        Iterator<Tuple2<MatrixIndexes, MatrixBlock>> aItr = a.iterator();
                        Tuple2<MatrixIndexes, MatrixBlock> value = null;
                        if (aItr.hasNext()) {
                            value = aItr.next();
                            MatrixBlock b1 = new MatrixBlock(value.f1);
                            long b1Nz = b1.getNonZeros();
                            while(aItr.hasNext()) {
                                value = aItr.next();
                                final MatrixBlock b2 = value.f1;
                                long b2Nz = b2.getNonZeros();

                                if (b1.getNumRows() != b2.getNumRows() || b1.getNumColumns() != b2.getNumColumns()) {
                                    throw new DMLRuntimeException("Mismatched block sizes for: "
                                            + b1.getNumRows() + " " + b1.getNumColumns() + " "
                                            + b2.getNumRows() + " " + b2.getNumColumns());
                                }

                                // execute merge (never pass by reference)
                                b1.merge(b2, false);
                                b1.examSparsity();

                                // sanity check output number of non-zeros
                                if (b1.getNonZeros() != b1Nz + b2Nz) {
                                    throw new DMLRuntimeException("Number of non-zeros does not match: "
                                            + b1.getNonZeros() + " != " + b1Nz + " + " + b2Nz);
                                }
                            }
                            collector.collect(new Tuple2<MatrixIndexes, MatrixBlock>(value.f0, b1));
                        }

                        aItr = b.iterator();
                        if (aItr.hasNext()) {
                            value = aItr.next();
                            MatrixBlock b1 = new MatrixBlock(value.f1);
                            long b1Nz = b1.getNonZeros();
                            while(aItr.hasNext()) {
                                value = aItr.next();
                                final MatrixBlock b2 = value.f1;
                                long b2Nz = b2.getNonZeros();

                                if (b1.getNumRows() != b2.getNumRows() || b1.getNumColumns() != b2.getNumColumns()) {
                                    throw new DMLRuntimeException("Mismatched block sizes for: "
                                            + b1.getNumRows() + " " + b1.getNumColumns() + " "
                                            + b2.getNumRows() + " " + b2.getNumColumns());
                                }

                                // execute merge (never pass by reference)
                                b1.merge(b2, false);
                                b1.examSparsity();

                                // sanity check output number of non-zeros
                                if (b1.getNonZeros() != b1Nz + b2Nz) {
                                    throw new DMLRuntimeException("Number of non-zeros does not match: "
                                            + b1.getNonZeros() + " != " + b1Nz + " + " + b2Nz);
                                }
                            }
                            collector.collect(new Tuple2<MatrixIndexes, MatrixBlock>(value.f0, b1));
                        }

                    }
                });

        return out;
    }

    public static DataSet<Tuple2<MatrixIndexes, MatrixBlock>> getPartialAggregates(ExecutionEnvironment env,
                                                                                   DataSet<String> input,
                                                                                   MatrixCharacteristics mcOut,
                                                                                   boolean hasHeader,
                                                                                   String delim,
                                                                                   boolean fill,
                                                                                   double fillValue, int offset) throws DMLRuntimeException{
        //determine unknown dimensions and sparsity if required
        if (!mcOut.dimsKnown(true)) {
            gatherStatistics(env, input, mcOut, hasHeader, delim);
        }

        //prepare csv w/ row indexes (sorted by filenames)
        DataSet<Tuple2<Long, String>> prepinput = DataSetUtils.zipWithIndex(input);

        //convert csv rdd to binary block rdd (w/ partial blocks)
        return prepinput.mapPartition(new CSVToBinaryBlockFunction(mcOut, delim, fill, fillValue, offset));
    }

    public static void gatherStatistics(ExecutionEnvironment env, DataSet<String> input, MatrixCharacteristics mcOut, boolean hasHeader, String delim) throws DMLRuntimeException {
        try {
            List<String> row = input.map(new CSVAnalysisFunction(delim)).first(1).collect();//.output(new DiscardingOutputFormat());
            JobExecutionResult result = env.getLastJobExecutionResult(); //env.execute("Calculate non zero values");
            long numRows = result.getAccumulatorResult(CSVAnalysisFunction.NUM_ROWS);
            numRows = numRows - (hasHeader ? 1 : 0);
            long numCols = row.get(0).split(delim).length;
            long nonZeroValues = result.getAccumulatorResult(CSVAnalysisFunction.NON_ZERO_VALUES);

            mcOut.set(numRows, numCols, mcOut.getRowsPerBlock(), mcOut.getColsPerBlock(), nonZeroValues);
        } catch (Exception e) {
            throw new DMLRuntimeException("Could not get metadata for input dataset: ", e);
        }
    }

    private static class CSVToBinaryBlockFunction
            implements MapPartitionFunction<Tuple2<Long, String>, Tuple2<MatrixIndexes, MatrixBlock>> {
        private static final long serialVersionUID = -4948430402942717043L;
        private final int offset;

        private long _rlen = -1;
        private long _clen = -1;
        private int _brlen = -1;
        private int _bclen = -1;
        private String _delim = null;
        private boolean _fill = false;
        private double _fillValue = 0;

        public CSVToBinaryBlockFunction(MatrixCharacteristics mc, String delim, boolean fill, double fillValue, int offset) {
            _rlen = mc.getRows();
            _clen = mc.getCols();
            _brlen = mc.getRowsPerBlock();
            _bclen = mc.getColsPerBlock();
            _delim = delim;
            _fill = fill;
            _fillValue = fillValue;
            this.offset = offset;
        }

        // Creates new state of empty column blocks for current global row index.
        private void createBlocks(long rowix, int lrlen, MatrixIndexes[] ix, MatrixBlock[] mb) {
            //compute row block index and number of column blocks
            long rix = UtilFunctions.computeBlockIndex(rowix, _brlen);
            int ncblks = (int) Math.ceil((double) _clen / _bclen);

            //create all column blocks (assume dense since csv is dense text format)
            for (int cix = 1; cix <= ncblks; cix++) {
                int lclen = (int) UtilFunctions.computeBlockSize(_clen, cix, _bclen);
                ix[cix - 1] = new MatrixIndexes(rix, cix + offset);
                mb[cix - 1] = new MatrixBlock(lrlen, lclen, false);
            }
        }

        // Flushes current state of filled column blocks to output list.
        private void flushBlocksToList(MatrixIndexes[] ix, MatrixBlock[] mb, Collector<Tuple2<MatrixIndexes, MatrixBlock>> out)
                throws DMLRuntimeException {
            int len = ix.length;
            for (int i = 0; i < len; i++)
                if (mb[i] != null) {
                    out.collect(new Tuple2<MatrixIndexes, MatrixBlock>(ix[i], mb[i]));
                    mb[i].examSparsity(); //ensure right representation
                }
        }

        @Override
        public void mapPartition(Iterable<Tuple2<Long, String>> values, Collector<Tuple2<MatrixIndexes, MatrixBlock>> out) throws Exception {

            int ncblks = (int) Math.ceil((double) _clen / _bclen);
            MatrixIndexes[] ix = new MatrixIndexes[ncblks];
            MatrixBlock[] mb = new MatrixBlock[ncblks];
            Iterator<Tuple2<Long, String>> arg0 = values.iterator();

            while (arg0.hasNext()) {
                Tuple2<Long, String> tmp = arg0.next();
                String row = tmp.f1;
                long rowix = tmp.f0 + 1;

                long rix = UtilFunctions.computeBlockIndex(rowix, _brlen);
                int pos = UtilFunctions.computeCellInBlock(rowix, _brlen);

                //create new blocks for entire row
                if (ix[0] == null || ix[0].getRowIndex() != rix) {
                    if (ix[0] != null)
                        flushBlocksToList(ix, mb, out);
                    long len = UtilFunctions.computeBlockSize(_rlen, rix, _brlen);
                    createBlocks(rowix, (int) len, ix, mb);
                }

                //process row data
                String[] parts = IOUtilFunctions.split(row, _delim);
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
            flushBlocksToList(ix, mb, out);
        }
    }

    private static class CSVAnalysisFunction extends RichMapFunction<String, String> {

        public static final String NUM_ROWS = "numRows";
        public static final String NON_ZERO_VALUES = "nonZeroValues";

        private final LongCounter numValues = new LongCounter();
        private final LongCounter nonZeroValues = new LongCounter();

        private final String delimiter;

        public CSVAnalysisFunction(String delim) {
            this.delimiter = delim;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            getRuntimeContext().addAccumulator(NUM_ROWS, this.numValues);
            getRuntimeContext().addAccumulator(NON_ZERO_VALUES, this.nonZeroValues);
        }

        @Override
        public String map(String line) throws Exception {
            //parse input line
            String[] cols = IOUtilFunctions.split(line, delimiter);

            //determine number of non-zeros of row (w/o string parsing)
            long lnnz = 0;
            for (String col : cols) {
                if (!col.isEmpty() && !col.equals("0") && !col.equals("0.0")) {
                    lnnz++;
                }
            }

            //update counters
            this.nonZeroValues.add(lnnz);
            this.numValues.add(1);

            return line;
        }
    }
}
