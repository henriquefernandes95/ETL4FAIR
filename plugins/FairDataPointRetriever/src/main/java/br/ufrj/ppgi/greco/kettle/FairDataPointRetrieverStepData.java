package br.ufrj.ppgi.greco.kettle;

import java.util.List;

import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;


/**
 * Responsável pelos dados do step FAIR Data Retriever.
 * 
 * @author Henrique Fernandes Rodrigues
 * 
 */

public class FairDataPointRetrieverStepData extends BaseStepData implements StepDataInterface {
	public RowMetaInterface outputRowMeta;
	public int inputRowSize;
	public List<Object[]> tripleList;

  	int outputFieldIndex = -1;

	public FairDataPointRetrieverStepData() {
		super();
	}
}
