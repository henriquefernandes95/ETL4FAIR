package br.ufrj.ppgi.greco.kettle;

import java.io.*;
import java.net.URL;
//import java.io.IOException;
//import java.io.PrintStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.impl.TreeModel;
import org.eclipse.rdf4j.model.util.Models;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.RDF4JException;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.config.RepositoryConfig;
import org.eclipse.rdf4j.repository.config.RepositoryConfigSchema;
import org.eclipse.rdf4j.repository.manager.RepositoryManager;
import org.eclipse.rdf4j.repository.manager.RepositoryProvider;
import org.eclipse.rdf4j.rio.*;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.StatementCollector;
import org.eclipse.rdf4j.rio.RDFHandler;

/**
 * Step Load Triple File.
 * 
 * Carrega um arquivo RDF em um banco de dados em grafos (GraphDB)
 * 
 * 
 * @author Nickolas Gomes Pinto
 * 
 */
public class LoadTripleFileStep extends BaseStep implements StepInterface {
	// Constantes
	public static final String LITERAL_OBJECT_TRIPLE_FORMAT = "<%s> <%s> \"%s\".";
	public static final String URI_OBJECT_TRIPLE_FORMAT = "<%s> <%s> <%s> .";
	public static final String RDF_TYPE_URI = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";

	public LoadTripleFileStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
			Trans trans) {
		super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
	}

	@Override
	public boolean init(StepMetaInterface smi, StepDataInterface sdi) {

		if (super.init(smi, sdi)) {
			return true;
		} else
			return false;

	} 

	@Override
	public void dispose(StepMetaInterface smi, StepDataInterface sdi) {
		super.dispose(smi, sdi);
	}

	/**
	 * Metodo chamado para cada linha que entra no step
	 */
	public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {
		LoadTripleFileStepMeta meta = (LoadTripleFileStepMeta) smi;
		LoadTripleFileStepData data = (LoadTripleFileStepData) sdi;

		// Obtem linha do fluxo de entrada e termina caso nao haja mais entrada

		Object[] row = getRow();

		if (row == null) { // Nao ha mais linhas de dados

			String inputFileFormat = meta.getInputFileFormat();
			System.out.println("inputFileFormat = " + inputFileFormat);
			String inputExistsRepository = meta.getExistsRepository();
			System.out.println("inputExistsRepository = " + inputExistsRepository);
			String inputRepoName = meta.getInputRepoName();
			System.out.println("inputRepoName = " + inputRepoName);
			String inputGraphName = meta.getInputGraph();
			System.out.println("inputGraphName = " + inputGraphName);
			String inputRepoURL = meta.getInputRepoURL();
			System.out.println("inputRepoURL = " + inputRepoURL);
			String browseFilename = meta.getBrowseFilename();
			System.out.println("browseFilename = " + browseFilename);
			setOutputDone();
			return false;
		}

		// Executa apenas uma vez. Variavel first definida na superclasse com
		// valor true
		if (first) {
			first = false;

			// Obtem todas as colunas ateh o step anterior.
			// Chamar apenas apos chamar getRow()
			RowMetaInterface rowMeta = getInputRowMeta();
			data.outputRowMeta = meta.getInnerKeepInputFields() ? rowMeta.clone() : new RowMeta();

			// Adiciona os metadados do step atual
			meta.getFields(data.outputRowMeta, getStepname(), null, null, this);
		}

		//String inputRepoURL;

		// Logica do step
		// Leitura de campos Input
		String inputFileFormat = meta.getInputFileFormat();
		System.out.println("inputFileFormat = " + inputFileFormat);
		String inputExistsRepository = meta.getExistsRepository();
		String inputRepoName = meta.getInputRepoName();
		String inputGraphName = meta.getInputGraph();
		String inputRepoURL = meta.getInputRepoURL();
		String browseFilename = meta.getBrowseFilename();

		try {
			
			log.logBasic("inputFileFormat = " + inputFileFormat);
			//System.out.println();

		} //catch (ParserConfigurationException e) {
		// 	// TODO Auto-generated catch block
		// 	e.printStackTrace();
		// } catch (SAXException e) {
		// 	// TODO Auto-generated catch block
		// 	e.printStackTrace();
		// } catch (IOException e) {
		// 	// TODO Auto-generated catch block
		// 	e.printStackTrace();
		// }
		catch (RuntimeException e) {
            System.out.print("RuntimeException: ");
            System.out.println(e.getMessage());
        }

		// if (inputExistsRepository.equals(RDF_TYPE_URI)) {
		// 	inputRepoURL = String.format(URI_OBJECT_TRIPLE_FORMAT, outputSubject, outputPredicate, outputObject);
		// } else {

		// 	inputRepoURL = String.format(LITERAL_OBJECT_TRIPLE_FORMAT, outputSubject, outputPredicate, outputObject);
		// }

		// Set output row
		Object[] outputRow = meta.getInnerKeepInputFields() ? row : new Object[0];

		outputRow = RowDataUtil.addValueData(outputRow, outputRow.length, inputRepoURL);

		putRow(data.outputRowMeta, outputRow);

		return true;
	}

	// Inicializa o Repositório
	public static RepositoryManager InitRepo(StepMetaInterface smi){

		 // Recupera variaveis da interface gráfica
		LoadTripleFileStepMeta meta = (LoadTripleFileStepMeta) smi;
		String inputRepoURL = meta.getInputRepoURL();

        String repo_path = inputRepoURL;
        RepositoryManager manager = RepositoryProvider.getRepositoryManager(repo_path);
        manager.init();
        manager.getAllRepositories();
        
        return manager;

    }

	// Cria repositório
	public static String CreateRepo(RepositoryManager manager, StepMetaInterface smi){

        // Recupera variaveis da interface gráfica
        LoadTripleFileStepMeta meta = (LoadTripleFileStepMeta) smi;
		String inputExistsRepository = meta.getExistsRepository(); // Verificador se repositorio existe ou nao
		String inputRepoName = meta.getInputRepoName(); // nome do repositorio

        // Verifica se vai usar repositorio ja existente ou criar um novo
        inputExistsRepository = inputExistsRepository.toUpperCase();

		// Cria variavel auxiliar para repo_name
		String repo_name = null;
        
        // Repositorio ja existe
        if ( inputExistsRepository.equals("S") ){

            repo_name = inputRepoName;
				
        } else if( inputExistsRepository.equals("N") ){ // Repositorio nao existe, cria um default

            InputStream config_test = null;
            RDFParser rdfParser_test = null;
            TreeModel graph_test = new TreeModel();

            try {

               config_test = new FileInputStream(new File("repo_config/repo-defaults_test.ttl"));

            } catch (FileNotFoundException e) {				
                e.printStackTrace();
            }

            rdfParser_test = Rio.createParser(RDFFormat.TURTLE);
           // rdfParser_test.setRDFHandler(new StatementCollector(graph_test));


            try {

                rdfParser_test.parse(config_test, RepositoryConfigSchema.NAMESPACE);

            } catch (IOException e) {
                e.printStackTrace();
            } 

            try {

                config_test.close();

            } catch (IOException e) {
                e.printStackTrace();
            }

            //Obtendo o repositório como recurso
            Resource repoNode_test = Models.subject(graph_test.filter(null, RDF.TYPE, RepositoryConfigSchema.REPOSITORY)).orElseThrow(() -> new RuntimeException("Oops, no <http://www.openrdf.org/config/repository#> subject found!"));

            //Adicionando as configurações
            RepositoryConfig configObj = RepositoryConfig.create(graph_test, repoNode_test);
            manager.addRepositoryConfig(configObj);

            repo_name = "repo_pdi";

        }

        return repo_name;

    }
}
