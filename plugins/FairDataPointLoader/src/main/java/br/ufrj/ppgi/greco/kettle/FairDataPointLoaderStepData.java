package br.ufrj.ppgi.greco.kettle;

import java.util.List;

import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;


/**
 * Responsável pelos dados do step FAIR Data Loader.
 * 
 * @author Henrique Fernandes Rodrigues
 * 
 */


public class FairDataPointLoaderStepData extends BaseStepData implements StepDataInterface {
	public RowMetaInterface outputRowMeta;
	public int inputRowSize;
	public List<Object[]> tripleList;

  	int outputFieldIndex = -1;

	// Tem que colocar em outra thread pois estava travando o Kettle
	// public CommThread commth;
	public FairDataPointLoaderStepData() {
		super();
	}
}
