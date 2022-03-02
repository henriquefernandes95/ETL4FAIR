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

import org.apache.http.entity.StringEntity;
import org.json.JSONObject;
import org.json.JSONException;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;

import java.util.regex.Pattern;
import java.util.regex.Matcher;

/**
 * Step FAIR Data Loader.
 * 
 * Carrega metadados em triplas em um FAIR Data Point
 * 
 * 
 * @author Henrique Fernandes Rodrigues
 * 
 */

public class FairDataPointLoaderStep extends BaseStep implements StepInterface {


	private boolean finished=false;
	String entradaTot="";
	int i=0;

	public FairDataPointLoaderStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr,
			TransMeta transMeta, Trans trans) {
		super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
	}

	public boolean init(StepMetaInterface smi, StepDataInterface sdi) {

		if (super.init(smi, sdi)) {
			return true;
		} else
			return false;

	} 

	public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {

		FairDataPointLoaderStepMeta meta = (FairDataPointLoaderStepMeta) smi;
		FairDataPointLoaderStepData data = (FairDataPointLoaderStepData) sdi;

		Object[] row = getRow();

		String elemento="";

		

		//HTTP_TEST
		String output,saida=null;
		BufferedReader br = null;
		
		if (row == null) { // Nao ha mais linhas de dados
			String typeLo = meta.getLoadType().toLowerCase();
			String inF = meta.getInputFileFormat();
			Boolean publishMeta = meta.getPublish();
			String fdpURL = meta.getURLfair();
			String raiz=null;
			Pattern pattern = Pattern.compile("(?:([^\\:]*)\\:\\/\\/)?(?:([^\\:\\@]*)(?:\\:([^\\@]*))?\\@)?(?:([^\\/\\:]*))?(?:\\:([0-9]*))?\\/");
			Matcher matcher = null;
			System.out.println("\n\n\n################"+typeLo+"################\n\n\n");
			System.out.println("\n\n\n################"+inF+"################\n\n\n");
			System.out.println("\nPUBLISH"+meta.getPublish());

			try {
				System.out.println(AuthorizeFAIRDP(fdpURL+"/tokens", "albert.einstein@example.com", "password"));
				elemento=gravaDados(fdpURL,entradaTot,typeLo, meta.getUsername(), meta.getPassword());
			}catch(IOException e) {
				e.printStackTrace();
			}

			if(publishMeta){
				System.out.println("\n\n\n################FDP URL"+fdpURL+"################\n\n\n");
				System.out.println("\n\n\n################ELEMENTO"+elemento+"################\n\n\n");
				
				try {
					publica(fdpURL,elemento, meta.getUsername(), meta.getPassword());
				}catch(IOException e) {
					e.printStackTrace();
				}
			}
			matcher = pattern.matcher(elemento);
			matcher.matches();
			matcher.find();
			raiz = "http"+matcher.group(0)+"n";
			System.out.println("\n\n\n################raiz"+raiz+"################\n\n\n");

			Object[] outputRow=null; 
			i=0;
			outputRow=RowDataUtil.addValueData(outputRow, i++,raiz);
			outputRow=RowDataUtil.addValueData(outputRow, i++,"http://purl.org/dc/terms/isPartOf");
			outputRow=RowDataUtil.addValueData(outputRow, i,elemento);
			putRow(data.outputRowMeta, outputRow);



			setOutputDone();
			return false;
		}
		

		// Executa apenas uma vez. Variavel first definida na superclasse com
		// valor true

		
		if (first) {
			int i = 0;
			first = false;
			
			
			// Obtem todas as colunas ateh o step anterior.
			// Chamar apenas apos chamar getRow()
			data.outputRowMeta = new RowMeta();

			// Adiciona os metadados do step atual
			meta.getFields(data.outputRowMeta, getStepname(), null, null, this);
		}
		if(row[i]!=null){
			System.out.println(row[i]);
			entradaTot+=row[i];
			//System.out.println("\nLOAD TYPE"+meta.getLoadType());
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

	private String acumulaEntrada(Object[] entrada){
		int i = 0;
		String entradaTot="";
		while(entrada[i] != null){
			entradaTot+=entrada[i];
			i++;
		}
		return entradaTot;
	}
	private String acumulaSaida(BufferedReader bfReader) throws IOException{
		String output;
		String outputTotal="";
		while ((output = bfReader.readLine()) != null) {
			outputTotal +=output;
		}
		return outputTotal;
	}

	public void processaTriplas(String resultadoGet, Object[] saidaProc, FairDataPointLoaderStepData dataStep) throws IOException, KettleStepException{
		String outputTot="";
        Statement statement;
        StringReader r;
        Iterator<Statement> iterator;

		Object[] outRow;

		
        r = new StringReader(resultadoGet);
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

	private String AuthorizeFAIRDP(String uri, String user, String password) throws IOException {
        
        HttpClient client2 = HttpClientBuilder.create().build();
        String jsonContent = "{\"email\": \""+user+"\", \"password\": \""+password+"\"}";//formato da requisição de autenticação em json
        StringEntity entity = new StringEntity(jsonContent);
        BufferedReader bReader = null;
        String line="";
        JSONObject obj = null;
        String token="";
        Boolean errorAuth=false;



        HttpPost postRequest = new HttpPost(uri);

        postRequest.setEntity(entity);
        
        postRequest.addHeader("accept", "application/json");
        postRequest.addHeader("Content-Type", "application/json");


        // Execute your request and catch response
        HttpResponse response = client2.execute(postRequest);

        bReader = new BufferedReader(new InputStreamReader((response.getEntity().getContent())));
        line = bReader.readLine();
        //System.out.println(line);

        obj = new JSONObject(line);

        try {
            token = obj.get("token").toString();
        }
        catch(JSONException jsonException){
            jsonException.printStackTrace();
            errorAuth=true;
			
        }
        if(errorAuth){
            System.out.println(obj.get("status"));
            System.out.println(obj.get("error"));
            System.out.println(obj.get("message"));
			logBasic("Erro de autenticação");
			logBasic(obj.get("status").toString());
			logBasic(obj.get("error").toString());
			logBasic(obj.get("message").toString());
        }
		else{
			logBasic("Autenticado com sucesso"+token.toString());
		}
		
        //System.out.println(token);
        return token;


    }

	private String gravaDados(String fdpURL, String content, String type, String user, String pass) throws IOException {//requisita autenticação por meio da função com esse fim e constroi POST para carga de dados
        HttpClient client3 = HttpClientBuilder.create().build();
        BufferedReader bReader = null;
        String token="";
        String output;
        int responseCode;
        String outputTot="";
        StringReader r;
        Iterator<Statement> iterator;
        Statement statement;
        String generatedElement = "";



        HttpPost postRequest = new HttpPost(fdpURL+"/"+type);//Tipo de POST no FAIR Data Point catalog, dataset ou distribution
        StringEntity entity = new StringEntity(content);

        token=AuthorizeFAIRDP(fdpURL+"/tokens",user,pass);

        postRequest.setEntity(entity);
        
        postRequest.addHeader("accept", "text/turtle");
        postRequest.addHeader("Content-Type", "text/n3");
        postRequest.addHeader("Authorization","Bearer "+token);

		logBasic(type);


        // Monta e executa request
        HttpResponse response = client3.execute(postRequest);
        responseCode = response.getStatusLine().getStatusCode();  // verifica resposta
        switch (responseCode) {
            case 201: {
                bReader = new BufferedReader(new InputStreamReader((response.getEntity().getContent())));
                while ((output = bReader.readLine()) != null) {
                    outputTot += output;
					
                }
				logBasic("Dados gravados com sucesso:"+outputTot);
                break;
            }
            case 400: {
				bReader = new BufferedReader(new InputStreamReader((response.getEntity().getContent())));
				System.out.println("Bad requestion. Parser failure or missing a FAIR metadata triple"+outputTot);
				logBasic("Resposta 400:Bad request. Falha no parser ou metadados FAIR faltando"+outputTot);
				while ((output = bReader.readLine()) != null) {
                    outputTot += output;
                    System.out.println(output);
                }
				
				logBasic(outputTot);
				break;
            }
            case 401: {
                System.out.println("Unauthorized");
				logBasic("Resposta 401: Não autorizado");
				break;
            }

        }
        r = new StringReader(outputTot);
        org.eclipse.rdf4j.model.Model model = Rio.parse(r,"",RDFFormat.TURTLE);
        iterator = model.iterator();

        if(iterator.hasNext()){
            statement=iterator.next();
            generatedElement= String.valueOf(statement.getSubject());//Obtém URI do objeto criado para reuso em sub-objetos
        }

        return generatedElement;

    }

	private String publica(String fdpURL, String elemento, String user, String pass)throws IOException{
		
        HttpClient client3 = HttpClientBuilder.create().build();
        String jsonContent = "{\"current\": \"PUBLISHED\"}";//formato da requisição de autenticação em json
        StringEntity entity = new StringEntity(jsonContent);
        BufferedReader bReader = null;
        String line="";
        JSONObject obj = null;
        String current="";
        Boolean errorAuth=false;
		String urlToUse=elemento;
		String token="";
		String outputTot="";
		String output;
		int responseCode;



		if(elemento.contains("localhost")){
			elemento=elemento.replace("http://localhost/",fdpURL);
			elemento=elemento.replace("https://localhost/",fdpURL);//para casos de https
		}
		elemento=elemento.replaceFirst("(?:([^\\:]*)\\:\\/\\/)?(?:([^\\:\\@]*)(?:\\:([^\\@]*))?\\@)?(?:([^\\/\\:]*))?(?:\\:([0-9]*))?\\/",fdpURL);
		System.out.println("\n\n\n################URL USED"+elemento+"################\n\n\n");
        HttpPut putRequest = new HttpPut(elemento+"/meta/state");

		token=AuthorizeFAIRDP(fdpURL+"tokens",user,pass);
		
        putRequest.setEntity(entity);
        
        putRequest.addHeader("accept", "application/json");
        putRequest.addHeader("Content-Type", "application/json");
		putRequest.addHeader("Authorization","Bearer "+token);


        // Execute your request and catch response
        HttpResponse response = client3.execute(putRequest);

        bReader = new BufferedReader(new InputStreamReader((response.getEntity().getContent())));
        line = bReader.readLine();
		obj = new JSONObject(line);

		responseCode = response.getStatusLine().getStatusCode();  // check the response code
        switch (responseCode) {
            case 201: {
                bReader = new BufferedReader(new InputStreamReader((response.getEntity().getContent())));
                while ((output = bReader.readLine()) != null) {
                    outputTot += output;
					
                }
				logBasic("Dados gravados com sucesso:"+outputTot);
                break;
            }
            case 400: {
				bReader = new BufferedReader(new InputStreamReader((response.getEntity().getContent())));
				System.out.println("Bad requestion. Parser failure or missing a FAIR metadata triple"+outputTot);
				logBasic("Resposta 400:Bad request. Falha no parser ou metadados FAIR faltando"+outputTot);
				while ((output = bReader.readLine()) != null) {
                    outputTot += output;
                    System.out.println(output);
                }
				
				logBasic(outputTot);
				break;
            }
            case 401: {
                System.out.println("Unauthorized");
				logBasic("Resposta 401: Não autorizado");
				break;
            }

        }


		try {
            current = obj.get("current").toString();
        }
        catch(JSONException jsonException){
            jsonException.printStackTrace();
            errorAuth=true;
			
        }
        if(errorAuth){
            
			logBasic("Erro de na publicação");

        }
		else{
			logBasic("Autenticado com sucesso"+current.toString());
		}
		
        return current;

	}

}

