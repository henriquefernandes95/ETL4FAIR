package br.ufrj.ppgi.greco.kettle;

import java.util.List;
import java.util.Map;

import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Counter;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaBoolean;
import org.pentaho.di.core.row.value.ValueMetaInteger;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.w3c.dom.Node;

/**
 * Interface de controle das variaveis do plugin FAIR Data Retriever
 * 
 * @author Henrique Fernandes Rodrigues
 * 
 */

public class FairDataPointRetrieverStepMeta extends BaseStepMeta implements StepMetaInterface {

	
  	private String outputField;

	// Fields for serialization
	public enum Field {
		RDF_CONTENT, GRAPH_URI, CLEAR_GRAPH, PROTOCOL, HOSTNAME, PORT, PATH, USERNAME, PASSWORD, ENDPOINT_URL, OUT_CODE, OUT_MESSAGE, TEST_URL
	}

	// Values - tipo refere-se ao tipo destas variaveis
	


	//adicionado
	private String testURL;
	//Campos de saida
	private String outputSubject;
	private String outputPredicate;
	private String outputObject;

	public FairDataPointRetrieverStepMeta() {
		setDefault();
	}

	// TODO Validar todos os campos para dar feedback ao usuario!
	@Override
	public void check(List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta, RowMetaInterface prev,
			String[] input, String[] output, RowMetaInterface info) {

		
		CheckResultInterface ok = new CheckResult(CheckResult.TYPE_RESULT_OK, "", stepMeta);
		remarks.add(ok);
	

	}

	public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr,
			TransMeta transMeta, Trans trans) {
		return new FairDataPointRetrieverStep(stepMeta, stepDataInterface, copyNr, transMeta, trans);
	}

	public StepDataInterface getStepData() {
		return new FairDataPointRetrieverStepData();
	}

	@Override
	public String getDialogClassName() {
		return FairDataPointRetrieverStepDialog.class.getName();
	}

	@Override
	public void loadXML(Node stepDomNode, List<DatabaseMeta> databases, Map<String, Counter> sequenceCounters)
			throws KettleXMLException {

		testURL = XMLHandler.getTagValue(stepDomNode, Field.TEST_URL.name());
	}

	@Override
	public String getXML() throws KettleException {
		StringBuilder xml = new StringBuilder();
		xml.append(XMLHandler.addTagValue(Field.TEST_URL.name(), testURL));
		return xml.toString();
	}

	@Override
	public void readRep(Repository repository, ObjectId stepIdInRepository, List<DatabaseMeta> databases,
			Map<String, Counter> sequenceCounters) throws KettleException {

		testURL = repository.getStepAttributeString(stepIdInRepository, Field.TEST_URL.name());
	}

	@Override
	public void saveRep(Repository repository, ObjectId idOfTransformation, ObjectId idOfStep) throws KettleException {

		repository.saveStepAttribute(idOfTransformation, idOfStep, Field.TEST_URL.name(), testURL);
	}

	public void setDefault() {
		

		outputSubject = "subject";
		outputPredicate = "predicate";
		outputObject = "object";

	}

	// Para os campos Field.OUT_*, refere-se ao tipo dos campos cujos nomes sao
	// especificados pelas estas variaveis desta classe Meta
	public int getFieldType(Field field) {
		if (field == Field.PORT)
			return ValueMetaInterface.TYPE_INTEGER;
		if (field == Field.OUT_CODE)
			return ValueMetaInterface.TYPE_INTEGER;
		else if (field == Field.CLEAR_GRAPH)
			return ValueMetaInterface.TYPE_BOOLEAN;
		else
			return ValueMetaInterface.TYPE_STRING;
	}

	public ValueMetaInterface getValueMeta(String name, Field field) {
		if (field == Field.PORT)
			return new ValueMetaInteger(name);
		if (field == Field.OUT_CODE)
			return new ValueMetaInteger(name);
		else if (field == Field.CLEAR_GRAPH)
			return new ValueMetaBoolean(name);
		else
			return new ValueMetaString(name);
	}

	/**
	 * it describes what each output row is going to look like
	 */
	@Override
	public void getFields(RowMetaInterface outputRowMeta, String name, RowMetaInterface[] info, StepMeta nextStep,
			VariableSpace space) throws KettleStepException {

		
		outputRowMeta.clear();
		addValueMeta(outputRowMeta, outputSubject, name);
		addValueMeta(outputRowMeta, outputPredicate, name);
		addValueMeta(outputRowMeta, outputObject, name);


	}

	private void addValueMeta(RowMetaInterface rowMeta, String fieldName, String origin) {
		//Criando os campos com seus metadados
		ValueMetaInterface field = new ValueMetaString(fieldName);
		field.setOrigin(origin);
		rowMeta.addValueMeta(field);
	}

	// Getters & Setters


	//funções criadas

	public void setTestUrl(String testURL) {
		this.testURL = testURL;
	}

	public String getTestURL() {
		return testURL;
	}

	public void setOutputField( String outputField ) {
		this.outputField = outputField;
	}

	public String getOutputField() {
		return outputField;
	}
}
