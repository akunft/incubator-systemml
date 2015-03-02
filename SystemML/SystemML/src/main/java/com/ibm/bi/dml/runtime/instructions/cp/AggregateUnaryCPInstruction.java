/**
 * IBM Confidential
 * OCO Source Materials
 * (C) Copyright IBM Corp. 2010, 2015
 * The source code for this program is not published or otherwise divested of its trade secrets, irrespective of what has been deposited with the U.S. Copyright Office.
 */

package com.ibm.bi.dml.runtime.instructions.cp;

import com.ibm.bi.dml.api.DMLScript;
import com.ibm.bi.dml.api.DMLScript.RUNTIME_PLATFORM;
import com.ibm.bi.dml.parser.Expression.DataType;
import com.ibm.bi.dml.parser.Expression.ValueType;
import com.ibm.bi.dml.runtime.DMLRuntimeException;
import com.ibm.bi.dml.runtime.DMLUnsupportedOperationException;
import com.ibm.bi.dml.runtime.controlprogram.caching.MatrixObject;
import com.ibm.bi.dml.runtime.controlprogram.context.ExecutionContext;
import com.ibm.bi.dml.runtime.functionobjects.Builtin;
import com.ibm.bi.dml.runtime.functionobjects.CM;
import com.ibm.bi.dml.runtime.instructions.Instruction;
import com.ibm.bi.dml.runtime.instructions.InstructionUtils;
import com.ibm.bi.dml.runtime.matrix.MatrixCharacteristics;
import com.ibm.bi.dml.runtime.matrix.data.MatrixBlock;
import com.ibm.bi.dml.runtime.matrix.data.MatrixIndexes;
import com.ibm.bi.dml.runtime.matrix.operators.AggregateUnaryOperator;
import com.ibm.bi.dml.runtime.matrix.operators.CMOperator;
import com.ibm.bi.dml.runtime.matrix.operators.Operator;
import com.ibm.bi.dml.runtime.matrix.operators.SimpleOperator;
import com.ibm.bi.dml.runtime.matrix.operators.CMOperator.AggregateOperationTypes;

public class AggregateUnaryCPInstruction extends UnaryCPInstruction
{
	@SuppressWarnings("unused")
	private static final String _COPYRIGHT = "Licensed Materials - Property of IBM\n(C) Copyright IBM Corp. 2010, 2015\n" +
                                             "US Government Users Restricted Rights - Use, duplication  disclosure restricted by GSA ADP Schedule Contract with IBM Corp.";
	
	public AggregateUnaryCPInstruction(Operator op, CPOperand in, CPOperand out, String opcode, String istr){
		this(op, in, null, null, out, opcode, istr);
	}
	
	public AggregateUnaryCPInstruction(Operator op, CPOperand in1, CPOperand in2, CPOperand out, String opcode, String istr){
		this(op, in1, in2, null, out, opcode, istr);
	}
	
	public AggregateUnaryCPInstruction(Operator op, CPOperand in1, CPOperand in2, CPOperand in3, CPOperand out, String opcode, String istr){
		super(op, in1, in2, in3, out, opcode, istr);
		_cptype = CPINSTRUCTION_TYPE.AggregateUnary;		
	}
	
	public static Instruction parseInstruction(String str)
		throws DMLRuntimeException {
		CPOperand in1 = new CPOperand("", ValueType.UNKNOWN, DataType.UNKNOWN);
		CPOperand in2 = null; 
		CPOperand in3 = null; 
		CPOperand out = new CPOperand("", ValueType.UNKNOWN, DataType.UNKNOWN);
		
		String opcode = InstructionUtils.getOpCode(str); 
		if ( opcode.equalsIgnoreCase("cm")) {
			String[] parts = InstructionUtils.getInstructionPartsWithValueType(str);
			if ( parts.length == 4 ) {
				// Example: CP.cm.mVar0.Var1.mVar2; (without weights)
				in2 = new CPOperand("", ValueType.UNKNOWN, DataType.UNKNOWN);
				parseUnaryInstruction(str, in1, in2, out);
			}
			else if ( parts.length == 5) {
				// CP.cm.mVar0.mVar1.Var2.mVar3; (with weights)
				in2 = new CPOperand("", ValueType.UNKNOWN, DataType.UNKNOWN);
				in3 = new CPOperand("", ValueType.UNKNOWN, DataType.UNKNOWN);
				parseUnaryInstruction(str, in1, in2, in3, out);
			}
		}
		else {
			parseUnaryInstruction(str, in1, out);
		}
		
		if(opcode.equalsIgnoreCase("nrow") || opcode.equalsIgnoreCase("ncol") || opcode.equalsIgnoreCase("length")){
			return new AggregateUnaryCPInstruction(new SimpleOperator(Builtin.getBuiltinFnObject(opcode)),
												   in1, 
												   out, 
												   opcode, str);
		}
		else if ( opcode.equalsIgnoreCase("cm")) {
			/* 
			 * Exact order of the central moment MAY NOT be known at compilation time.
			 * We first try to parse the second argument as an integer, and if we fail, 
			 * we simply pass -1 so that getCMAggOpType() picks up AggregateOperationTypes.INVALID.
			 * It must be updated at run time in processInstruction() method.
			 */
			
			int cmOrder;
			try {
				if ( in3 == null ) {
					cmOrder = Integer.parseInt(in2.getName());
				}
				else {
					cmOrder = Integer.parseInt(in3.getName());
				}
			} catch(NumberFormatException e) {
				cmOrder = -1; // unknown at compilation time
			}
			
			AggregateOperationTypes opType = CMOperator.getCMAggOpType(cmOrder);
			CMOperator cm = new CMOperator(CM.getCMFnObject(opType), opType);
			return new AggregateUnaryCPInstruction(cm, in1, in2, in3, out, opcode, str);
		}
		else //DEFAULT BEHAVIOR
		{
			AggregateUnaryOperator aggun = InstructionUtils.parseBasicAggregateUnaryOperator(opcode);
			return new AggregateUnaryCPInstruction(aggun, in1, out, opcode, str);				
		}
	}
	
	@Override
	public void processInstruction( ExecutionContext ec )
		throws DMLRuntimeException, DMLUnsupportedOperationException
	{
		String output_name = output.getName();
		String opcode = getOpcode();
		
		if( opcode.equalsIgnoreCase("nrow") || opcode.equalsIgnoreCase("ncol") || opcode.equalsIgnoreCase("length")  )
		{
			//get meta data information
			MatrixCharacteristics mc = ec.getMatrixCharacteristics(input1.getName());
			long rval = -1;
			if(opcode.equalsIgnoreCase("nrow"))
				rval = mc.getRows();
			else if(opcode.equalsIgnoreCase("ncol"))
				rval = mc.getCols();
			else if(opcode.equalsIgnoreCase("length"))
				rval = mc.getRows() * mc.getCols();

			//check for valid output, and acquire read if necessary
			//(Use case: In case of forced exec type singlenode, there are no reblocks. For csv
			//we however, support unspecified input sizes, which requires a read to obtain the
			//required meta data)
			//Note: check on matrix characteristics to cover incorrect length (-1*-1 -> 1)
			if( !mc.dimsKnown() ) //invalid nrow/ncol/length
			{
				if( DMLScript.rtplatform == RUNTIME_PLATFORM.SINGLE_NODE )
				{
					//read the input data and explicitly refresh input data
					MatrixObject mo = (MatrixObject)ec.getVariable(input1.getName());
					mo.acquireRead();
					mo.refreshMetaData();
					mo.release();
					
					//update meta data information
					mc = ec.getMatrixCharacteristics(input1.getName());
					if(opcode.equalsIgnoreCase("nrow"))
						rval = mc.getRows();
					else if(opcode.equalsIgnoreCase("ncol"))
						rval = mc.getCols();
					else if(opcode.equalsIgnoreCase("length"))
						rval = mc.getRows() * mc.getCols();
				}
				else {
					throw new DMLRuntimeException("Invalid meta data returned by '"+opcode+"': "+rval);
				}
			}
			
			//create and set output scalar
			ScalarObject ret = null;
			switch( output.getValueType() ) {
				case INT:	  ret = new IntObject(output_name, rval); break;
				case DOUBLE:  ret = new DoubleObject(output_name, rval); break;
				case STRING:  ret = new StringObject(output_name, String.valueOf(rval)); break;
			}
			ec.setScalarOutput(output_name, ret);
			return;
		}
		else if (opcode.equalsIgnoreCase("cm")) 
		{
			/*
			 * The "order" of the central moment in the instruction can 
			 * be set to INVALID when the exact value is unknown at 
			 * compilation time. We first need to determine the exact 
			 * order and update the CMOperator, if needed.
			 */
			
			MatrixBlock matBlock = ec.getMatrixInput(input1.getName());
	
			CPOperand scalarInput = (input3==null ? input2 : input3);
			ScalarObject order = ec.getScalarInput(scalarInput.getName(), scalarInput.getValueType(), scalarInput.isLiteral()); 
			
			CMOperator cm_op = ((CMOperator)_optr); 
			if ( cm_op.getAggOpType() == AggregateOperationTypes.INVALID ) {
				((CMOperator)_optr).setCMAggOp((int)order.getLongValue());
			}
			
			CM_COV_Object cmobj = null; 
			if (input3 == null ) {
				cmobj = matBlock.cmOperations(cm_op);
			}
			else {
				MatrixBlock wtBlock = ec.getMatrixInput(input2.getName());
				cmobj = matBlock.cmOperations(cm_op, wtBlock);
				ec.releaseMatrixInput(input2.getName());
			}
			
			matBlock = null;
			ec.releaseMatrixInput(input1.getName());
			
			double val = cmobj.getRequiredResult(_optr);
			DoubleObject ret = new DoubleObject(output_name, val);
			
			ec.setScalarOutput(output_name, ret);
			return;
		} 
		else 
		{
			/* Default behavior for AggregateUnary Instruction */
			MatrixBlock matBlock = ec.getMatrixInput(input1.getName());		
			AggregateUnaryOperator au_op = (AggregateUnaryOperator) _optr;
			
			MatrixBlock resultBlock = (MatrixBlock) matBlock.aggregateUnaryOperations(au_op, new MatrixBlock(), matBlock.getNumRows(), matBlock.getNumColumns(), new MatrixIndexes(1, 1), true);
			
			ec.releaseMatrixInput(input1.getName());
			
			if(output.getDataType() == DataType.SCALAR){
				DoubleObject ret = new DoubleObject(output_name, resultBlock.getValue(0, 0));
				ec.setScalarOutput(output_name, ret);
			} else{
				// since the computed value is a scalar, allocate a "temp" output matrix
				ec.setMatrixOutput(output_name, resultBlock);
			}
		}
	}

}