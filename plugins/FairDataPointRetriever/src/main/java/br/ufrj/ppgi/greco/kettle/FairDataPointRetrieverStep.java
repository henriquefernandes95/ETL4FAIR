package br.ufrj.ppgi.greco.kettle;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;

import org.apache.http.Header;
import org.apache.http.HeaderIterator;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

import org.pentaho.di.core.row.RowMeta;



import java.io.IOException;
import org.apache.http.client.ClientProtocolException;

import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.HttpResponse;

import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import java.io.*;
import java.util.Iterator;

/**
 * Step FAIR Data Retriever.
 * 
 * Recupera metadados em triplas em um FAIR Data Point
 * 
 * 
 * @author Henrique Fernandes Rodrigues
 * 
 */


public class FairDataPointRetrieverStep extends BaseStep implements StepInterface {

	

	private boolean finished=false;

	public FairDataPointRetrieverStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr,
			TransMeta transMeta, Trans trans) {
		super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
	}

	public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {

		FairDataPointRetrieverStepMeta meta = (FairDataPointRetrieverStepMeta) smi;
		FairDataPointRetrieverStepData data = (FairDataPointRetrieverStepData) sdi;

		Object[] row = getRow();

		//teste

		System.out.println("\n\n\n################"+environmentSubstitute(meta.getTestURL())+"################\n\n\n");

	

		//HTTP_TEST
		String output,saida=null;
		BufferedReader br = null;
		br = ProcessaGET(environmentSubstitute(meta.getTestURL()));
		
		try{
			saida = acumulaSaida(br);
		}catch (IOException e) {
			e.printStackTrace();
		}
		//row==null define o fim do processamento do step e que processRow() não deve ser chamado novamente
		//finalização substituída pela flag finished que aponta a finalização do processo dos dados obtidos
		if (finished) {
			System.out.println("Finished");
			
			

			setOutputDone();
			return false;
		}
		//manter first. Parte da arquitetura PDI
		if (first) { // Executa apenas uma vez, first eh definido na superclasse
			first = false;
			data.outputRowMeta = new RowMeta();

			// Adiciona os metadados do step atual
			meta.getFields(data.outputRowMeta, getStepname(), null, null, this);
			
		}
		

		Object[] outputRow=null; 
		try{
			processaTriplas(saida,outputRow, data);
		}catch (IOException e) {
			e.printStackTrace();
		}
		
		return true;
	}

	

	
	
	

	//CHECK DISPOSE
	//super.dispose(meta,data);//mandatório da arquitetura PDI para funcionamento correto

	private BufferedReader ProcessaGET(String uri){
		BufferedReader bReader = null;
		try {
			HttpClient client = HttpClientBuilder.create().build();
	  
			HttpGet getRequest = new HttpGet(uri);
	  
			// Header do request, definindo a captura
			getRequest.addHeader("accept", "text/n3");//Captura NTriple
	  
	  
			// Executa e obtém resposta
			HttpResponse response = client.execute(getRequest);
	  
			
			//Obtém a resposta para um leitor
			bReader = new BufferedReader(new InputStreamReader((response.getEntity().getContent())));
	  
			System.out.println("============Output:============");
	  
			
		}catch (ClientProtocolException e) {
			e.printStackTrace();
	  
		} catch (IOException e) {
			e.printStackTrace();
		}

		return bReader;
	}

	private String acumulaSaida(BufferedReader bfReader) throws IOException{
		String output;
		String outputTotal="";
		while ((output = bfReader.readLine()) != null) {
			outputTotal +=output;
		}
		return outputTotal;
	}

	public void processaTriplas(String resultadoGet, Object[] saidaProc, FairDataPointRetrieverStepData dataStep) throws IOException, KettleStepException{
		String outputTot="";
        Statement statement;
        StringReader r;
        Iterator<Statement> iterator;

		Object[] outRow;

		
        r = new StringReader(resultadoGet);
		System.out.println(r);
        org.eclipse.rdf4j.model.Model model = Rio.parse(r,"",RDFFormat.N3);
        iterator = model.iterator();
		
        while(iterator.hasNext()){
			//Obtem a tripla, statement, e a quebra em sujeito predicado e objeto
			//carrega cada componente para uma variável, coluna, no PDI. Processo feito com a classe Meta
			int i = 0;
            statement=iterator.next();
            System.out.println("subject"+statement.getSubject());
            System.out.println("property"+statement.getPredicate());
            System.out.println("object"+statement.getObject());
			System.out.println("number"+i);
			saidaProc = RowDataUtil.addValueData(saidaProc, i++, statement.getSubject());
			saidaProc = RowDataUtil.addValueData(saidaProc, i++, statement.getPredicate());
			saidaProc = RowDataUtil.addValueData(saidaProc, i++, statement.getObject());
			putRow(dataStep.outputRowMeta, saidaProc);
        }
		finished=true;


	}

}

