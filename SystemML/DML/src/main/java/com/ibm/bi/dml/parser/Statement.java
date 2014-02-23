/**
 * IBM Confidential
 * OCO Source Materials
 * (C) Copyright IBM Corp. 2010, 2014
 * The source code for this program is not published or otherwise divested of its trade secrets, irrespective of what has been deposited with the U.S. Copyright Office.
 */

package com.ibm.bi.dml.parser;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public abstract class Statement 
{
	@SuppressWarnings("unused")
	private static final String _COPYRIGHT = "Licensed Materials - Property of IBM\n(C) Copyright IBM Corp. 2010, 2013\n" +
                                             "US Government Users Restricted Rights - Use, duplication  disclosure restricted by GSA ADP Schedule Contract with IBM Corp.";
	
	protected static final Log LOG = LogFactory.getLog(Statement.class.getName());
	
	public static final String INPUTSTATEMENT = "read";
	public static final String OUTPUTSTATEMENT = "write";
	public static final String PRINTSTATEMENT = "print";
	
	public static final String IO_FILENAME = "iofilename";
	public static final String READROWPARAM = "rows";
	public static final String READCOLPARAM = "cols";
	public static final String READNUMNONZEROPARAM = "nnz";
	
	public static final String FORMAT_TYPE 						= "format";
	public static final String FORMAT_TYPE_VALUE_TEXT 			= "text";
	public static final String FORMAT_TYPE_VALUE_BINARY 		= "binary";
	public static final String FORMAT_TYPE_VALUE_CSV			= "csv";
	public static final String FORMAT_TYPE_VALUE_MATRIXMARKET	= "mm";
	
	public static final String DELIM_DELIMITER = "sep";
	public static final String DELIM_HAS_HEADER_ROW = "header";
	public static final String DELIM_FILL = "fill";
	public static final String DELIM_FILL_VALUE = "default";
	public static final String DELIM_SPARSE = "sparse";
	
	/* Default Values for delimited (CSV) files */
	public static final String  DEFAULT_DELIM_DELIMITER = ",";
	public static final boolean DEFAULT_DELIM_HAS_HEADER_ROW = false;
	public static final boolean DEFAULT_DELIM_FILL = true;
	public static final double  DEFAULT_DELIM_FILL_VALUE = 0.0;
	public static final boolean DEFAULT_DELIM_SPARSE = true;
	
	public static final String ROWBLOCKCOUNTPARAM = "rows_in_block";
	public static final String COLUMNBLOCKCOUNTPARAM = "cols_in_block";
	public static final String DATATYPEPARAM = "data_type";
	public static final String VALUETYPEPARAM = "value_type";
	public static final String DESCRIPTIONPARAM = "description"; 

	public static final String RAND_ROWS 	=  "rows";	 
	public static final String RAND_COLS 	=  "cols";
	public static final String RAND_MIN  	=  "min";
	public static final String RAND_MAX  	=  "max";
	public static final String RAND_SPARSITY = "sparsity"; 
	public static final String RAND_SEED    =  "seed";
	public static final String RAND_PDF		=  "pdf";
	
	// parameter names for seq()
	public static final String SEQ_FROM = "from"; 
	public static final String SEQ_TO   = "to";
	public static final String SEQ_INCR	= "incr";
	
	public static final String RAND_BY_ROW 	 =  "byrow";	 
	public static final String RAND_DIMNAMES =  "dimnames";
	public static final String RAND_DATA 	 =  "data";
	
	public static final String SOURCE  	= "source";
	public static final String SETWD 	= "setwd";

	public static final String MATRIX_DATA_TYPE = "matrix";
	public static final String SCALAR_DATA_TYPE = "scalar";
	
	public static final String DOUBLE_VALUE_TYPE = "double";
	public static final String BOOLEAN_VALUE_TYPE = "boolean";
	public static final String INT_VALUE_TYPE = "int";
	public static final String STRING_VALUE_TYPE = "string";
	
	// String constants related to Grouped Aggregate parameters
	public static final String GAGG_TARGET  = "target";
	public static final String GAGG_GROUPS  = "groups";
	public static final String GAGG_WEIGHTS = "weights";
	public static final String GAGG_FN      = "fn";
	public static final String GAGG_FN_SUM      = "sum";
	public static final String GAGG_FN_COUNT    = "count";
	public static final String GAGG_FN_MEAN     = "mean";
	public static final String GAGG_FN_VARIANCE = "variance";
	public static final String GAGG_FN_CM       = "centralmoment";
	public static final String GAGG_FN_CM_ORDER = "order";
	
	public abstract boolean controlStatement();
	
	public abstract VariableSet variablesRead();
	public abstract VariableSet variablesUpdated();
 
	public abstract void initializeforwardLV(VariableSet activeIn) throws LanguageException;
	public abstract VariableSet initializebackwardLV(VariableSet lo) throws LanguageException;
	
	public abstract Statement rewriteStatement(String prefix) throws LanguageException;
	
	
	///////////////////////////////////////////////////////////////////////////
	// store exception info + position information for statements
	///////////////////////////////////////////////////////////////////////////
	public int _beginLine, _beginColumn;
	public int _endLine, _endColumn;
	
	public void setBeginLine(int passed)    { _beginLine = passed;   }
	public void setBeginColumn(int passed) 	{ _beginColumn = passed; }
	public void setEndLine(int passed) 		{ _endLine = passed;   }
	public void setEndColumn(int passed)	{ _endColumn = passed; }
	
	public void setAllPositions(int blp, int bcp, int elp, int ecp){
		_beginLine	 = blp; 
		_beginColumn = bcp; 
		_endLine 	 = elp;
		_endColumn 	 = ecp;
	}

	public int getBeginLine()	{ return _beginLine;   }
	public int getBeginColumn() { return _beginColumn; }
	public int getEndLine() 	{ return _endLine;   }
	public int getEndColumn()	{ return _endColumn; }
	
	public String printErrorLocation(){
		return "ERROR: line " + _beginLine + ", column " + _beginColumn + " -- ";
	}
	
	public String printWarningLocation(){
		return "WARNING: line " + _beginLine + ", column " + _beginColumn + " -- ";
	}
	
	public String printInfoLocation(){
		return "INFO: line " + _beginLine + ", column " + _beginColumn + " -- ";
	}
	public String printErrorLocation(int beginLine, int beginColumn){
		return "ERROR: line " + beginLine + ", column " + beginColumn + " -- ";
	}
	
	public String printWarningLocation(int beginLine, int beginColumn){
		return "WARNING: line " + beginLine + ", column " + beginColumn + " -- ";
	}
	
	public String printInfoLocation(int beginLine, int beginColumn){
		return "INFO: line " + beginLine + ", column " + beginColumn + " -- ";
	}
	
	
}