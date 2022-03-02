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
 * Interface de controle das variaveis do plugin FAIR Data Loader
 * 
 * @author Henrique Fernandes Rodrigues
 * 
 */

public class FairDataPointLoaderStepMeta extends BaseStepMeta implements StepMetaInterface {

	
  	private String outputField;

	// Fields for serialization
	public enum Field {
		RDF_CONTENT, GRAPH_URI, CLEAR_GRAPH, PROTOCOL, HOSTNAME, PORT, PATH, USERNAME, PASSWORD, ENDPOINT_URL, OUT_CODE, OUT_MESSAGE, TEST_URL, LOAD_TYPE_FIELD_NAME,INPUT_FILE_FORMAT_FIELD_NAME,FAIRDP_URL,PUBLISH,
	}

	// Values - tipo refere-se ao tipo destas variaveis
	/*private String rdfContentFieldName;
	private String graphUriValue;
	private Boolean clearGraph;
	private String endpointUrl;*/
	private String username;
	private String password;
	// Output - ATENCAO: tipo refere-se ao tipo dos campos cujos nomes sao
	// especificados por estas variaveis
	private String resultCodeFieldName;
	private String resultMessageFieldName;


	//adicionado
	private String testURL;
	//Campos de saida
	private String outputSubject;
	private String outputPredicate;
	private String outputObject;

	private String outputGenerated;
	private String loadType;
	private String inputFileFormat;
	private String fairdp;

	private String subGen;
	private String predGen;

	private Boolean publish;
	private String urlFAIR;

	public FairDataPointLoaderStepMeta() {
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
		return new FairDataPointLoaderStep(stepMeta, stepDataInterface, copyNr, transMeta, trans);
	}

	public StepDataInterface getStepData() {
		return new FairDataPointLoaderStepData();
	}

	@Override
	public String getDialogClassName() {
		return FairDataPointLoaderStepDialog.class.getName();
	}

	@Override
	public void loadXML(Node stepDomNode, List<DatabaseMeta> databases, Map<String, Counter> sequenceCounters)
			throws KettleXMLException {

		/*rdfContentFieldName = XMLHandler.getTagValue(stepDomNode, Field.RDF_CONTENT.name());
		graphUriValue = XMLHandler.getTagValue(stepDomNode, Field.GRAPH_URI.name());
		clearGraph = "Y".equals(XMLHandler.getTagValue(stepDomNode, Field.CLEAR_GRAPH.name()));
		endpointUrl = XMLHandler.getTagValue(stepDomNode, Field.ENDPOINT_URL.name());*/
		username = XMLHandler.getTagValue(stepDomNode, Field.USERNAME.name());
		password = XMLHandler.getTagValue(stepDomNode, Field.PASSWORD.name());/*
		resultCodeFieldName = XMLHandler.getTagValue(stepDomNode, Field.OUT_CODE.name());
		resultMessageFieldName = XMLHandler.getTagValue(stepDomNode, Field.OUT_MESSAGE.name());*/
		testURL = XMLHandler.getTagValue(stepDomNode, Field.TEST_URL.name());
		loadType = XMLHandler.getTagValue(stepDomNode, Field.LOAD_TYPE_FIELD_NAME.name());
		inputFileFormat = XMLHandler.getTagValue(stepDomNode, Field.INPUT_FILE_FORMAT_FIELD_NAME.name());
		//endpointUrl = XMLHandler.getTagValue(stepDomNode, Field.ENDPOINT_URL.name());
		urlFAIR = XMLHandler.getTagValue(stepDomNode, Field.FAIRDP_URL.name());
		publish = "Y".equals(XMLHandler.getTagValue(stepDomNode, Field.PUBLISH.name()));
	}

	@Override
	public String getXML() throws KettleException {
		StringBuilder xml = new StringBuilder();
		/*xml.append(XMLHandler.addTagValue(Field.RDF_CONTENT.name(), rdfContentFieldName));
		xml.append(XMLHandler.addTagValue(Field.GRAPH_URI.name(), graphUriValue));
		xml.append(XMLHandler.addTagValue(Field.CLEAR_GRAPH.name(), clearGraph));
		xml.append(XMLHandler.addTagValue(Field.ENDPOINT_URL.name(), endpointUrl));*/
		xml.append(XMLHandler.addTagValue(Field.USERNAME.name(), username));
		xml.append(XMLHandler.addTagValue(Field.PASSWORD.name(), password));/*
		xml.append(XMLHandler.addTagValue(Field.OUT_CODE.name(), resultCodeFieldName));
		xml.append(XMLHandler.addTagValue(Field.OUT_MESSAGE.name(), resultMessageFieldName));*/
		xml.append(XMLHandler.addTagValue(Field.TEST_URL.name(), testURL));
		xml.append(XMLHandler.addTagValue(Field.LOAD_TYPE_FIELD_NAME.name(), loadType));
		xml.append(XMLHandler.addTagValue(Field.INPUT_FILE_FORMAT_FIELD_NAME.name(), inputFileFormat));
		xml.append(XMLHandler.addTagValue(Field.FAIRDP_URL.name(), urlFAIR));
		xml.append(XMLHandler.addTagValue(Field.PUBLISH.name(), publish));

		return xml.toString();
	}

	@Override
	public void readRep(Repository repository, ObjectId stepIdInRepository, List<DatabaseMeta> databases,
			Map<String, Counter> sequenceCounters) throws KettleException {

		/*rdfContentFieldName = repository.getStepAttributeString(stepIdInRepository, Field.RDF_CONTENT.name());
		graphUriValue = repository.getStepAttributeString(stepIdInRepository, Field.GRAPH_URI.name());
		clearGraph = repository.getStepAttributeBoolean(stepIdInRepository, Field.CLEAR_GRAPH.name());
		endpointUrl = repository.getStepAttributeString(stepIdInRepository, Field.ENDPOINT_URL.name());
		*/username = repository.getStepAttributeString(stepIdInRepository, Field.USERNAME.name());
		password = repository.getStepAttributeString(stepIdInRepository, Field.PASSWORD.name());
		/*resultCodeFieldName = repository.getStepAttributeString(stepIdInRepository, Field.OUT_CODE.name());
		resultMessageFieldName = repository.getStepAttributeString(stepIdInRepository, Field.OUT_MESSAGE.name());*/
		testURL = repository.getStepAttributeString(stepIdInRepository, Field.TEST_URL.name());
		loadType = repository.getStepAttributeString(stepIdInRepository, Field.LOAD_TYPE_FIELD_NAME.name());
		inputFileFormat = repository.getStepAttributeString(stepIdInRepository, Field.INPUT_FILE_FORMAT_FIELD_NAME.name());
		urlFAIR = repository.getStepAttributeString(stepIdInRepository, Field.FAIRDP_URL.name());
		publish = repository.getStepAttributeBoolean(stepIdInRepository, Field.PUBLISH.name());
	}

	@Override
	public void saveRep(Repository repository, ObjectId idOfTransformation, ObjectId idOfStep) throws KettleException {

		/*repository.saveStepAttribute(idOfTransformation, idOfStep, Field.RDF_CONTENT.name(), rdfContentFieldName);
		repository.saveStepAttribute(idOfTransformation, idOfStep, Field.GRAPH_URI.name(), graphUriValue);
		repository.saveStepAttribute(idOfTransformation, idOfStep, Field.CLEAR_GRAPH.name(), clearGraph);
		repository.saveStepAttribute(idOfTransformation, idOfStep, Field.ENDPOINT_URL.name(), endpointUrl);
		repository.saveStepAttribute(idOfTransformation, idOfStep, Field.USERNAME.name(), username);
		repository.saveStepAttribute(idOfTransformation, idOfStep, Field.PASSWORD.name(), password);
		repository.saveStepAttribute(idOfTransformation, idOfStep, Field.OUT_CODE.name(), resultCodeFieldName);
		repository.saveStepAttribute(idOfTransformation, idOfStep, Field.OUT_MESSAGE.name(), resultMessageFieldName);*/
		repository.saveStepAttribute(idOfTransformation, idOfStep, Field.TEST_URL.name(), testURL);
		repository.saveStepAttribute(idOfTransformation, idOfStep, Field.LOAD_TYPE_FIELD_NAME.name(), loadType);
		repository.saveStepAttribute(idOfTransformation, idOfStep, Field.INPUT_FILE_FORMAT_FIELD_NAME.name(), inputFileFormat);
		repository.saveStepAttribute(idOfTransformation, idOfStep, Field.FAIRDP_URL.name(), inputFileFormat);
		repository.saveStepAttribute(idOfTransformation, idOfStep, Field.PUBLISH.name(), publish);
	}

	public void setDefault() {
		/*graphUriValue = "";
		rdfContentFieldName = "";
		clearGraph = true;*/

		publish = true;

		//endpointUrl = "http://example.com:80/sparql-auth";
		username = "username";
		password = "";

		loadType="";
		fairdp="";

		/*resultCodeFieldName = "status_code";
		resultMessageFieldName = "status_message";*/

		outputGenerated = "Generated Element";

		subGen = "subject";//"http://localhost/n";
		predGen = "predicate";//"http://purl.org/dc/terms/isPartOf";

		
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
		
		addValueMeta(outputRowMeta, subGen, name);
		addValueMeta(outputRowMeta, predGen, name);
		addValueMeta(outputRowMeta, outputGenerated, name);


	}

	private void addValueMeta(RowMetaInterface rowMeta, String fieldName, String origin) {
		//Criando os campos com seus metadados
		ValueMetaInterface field = new ValueMetaString(fieldName);
		field.setOrigin(origin);
		rowMeta.addValueMeta(field);
	}

	// Getters & Setters

	/*public String getRdfContentFieldName() {
		return rdfContentFieldName;
	}

	public void setRdfContentFieldName(String value) {
		rdfContentFieldName = value;
	}

	public String getGraphUriValue() {
		return graphUriValue;
	}

	public void setGraphUriValue(String graphUriValue) {
		this.graphUriValue = graphUriValue;
	}

	public Boolean getClearGraph() {
		return clearGraph;
	}

	public void setClearGraph(Boolean clearGraph) {
		this.clearGraph = clearGraph;
	}

	public String getEndpointUrl() {
		return endpointUrl;
	}

	public void setEndpointUrl(String endpointUrl) {
		this.endpointUrl = endpointUrl;
	}*/

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}
/*
	public String getResultCodeFieldName() {
		return resultCodeFieldName;
	}

	public void setResultCodeFieldName(String resultCodeFieldName) {
		this.resultCodeFieldName = resultCodeFieldName;
	}

	public String getResultMessageFieldName() {
		return resultMessageFieldName;
	}

	public void setResultMessageFieldName(String resultMessageFieldName) {
		this.resultMessageFieldName = resultMessageFieldName;
	}*/

	//funções criadas

	public void setURLfair(String urlFAIR) {
		this.urlFAIR = urlFAIR;
	}

	public String getURLfair() {
		return urlFAIR;
	}

	public void setOutputField( String outputField ) {
		this.outputField = outputField;
	}

	public String getOutputField() {
		return outputField;
	}
	public String getLoadType() {
		return loadType;
	}
	public void setLoadType(String loadType) {
		this.loadType = loadType;
	}
	public String getInputFileFormat() {
		return inputFileFormat;
	}

	public void setInputFileFormat(String inputFileFormat) {
		this.inputFileFormat = inputFileFormat;
	}

	public String getFAIRDP() {
		return fairdp;
	}
	public void setFAIRDP(String fairdp) {
		this.fairdp = fairdp;
	}

	public Boolean getPublish() {
		return publish;
	}

	public void setPublish(Boolean publish) {
		this.publish = publish;
	}
}
