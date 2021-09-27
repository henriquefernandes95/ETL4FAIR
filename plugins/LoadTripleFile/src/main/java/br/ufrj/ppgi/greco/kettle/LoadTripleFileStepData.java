package br.ufrj.ppgi.greco.kettle;

import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

/**
 * Guarda dados usados durante processamento do step Annotator.
 * 
 * @author Nickolas Gomes Pinto
 * 
 */
public class LoadTripleFileStepData extends BaseStepData implements StepDataInterface {
	public RowMetaInterface outputRowMeta;
}
