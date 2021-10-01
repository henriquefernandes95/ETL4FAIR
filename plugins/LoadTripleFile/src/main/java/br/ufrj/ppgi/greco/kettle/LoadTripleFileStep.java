package br.ufrj.ppgi.greco.kettle;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;

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
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.config.RepositoryConfig;
import org.eclipse.rdf4j.repository.config.RepositoryConfigSchema;
import org.eclipse.rdf4j.repository.manager.RepositoryManager;
import org.eclipse.rdf4j.repository.manager.RepositoryProvider;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.StatementCollector;

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

	public static RepositoryManager InitRepo(StepMetaInterface smi){

        // // Recupera variaveis do teclado
        // Scanner keyboard = new Scanner(System.in);

        // //inicializa o repositório
        // System.out.println("Entre com a URL do repositório: ");
		LoadTripleFileStepMeta meta = (LoadTripleFileStepMeta) smi;
		String inputRepoURL = meta.getInputRepoURL();
        String repo_path = inputRepoURL;
        RepositoryManager manager = RepositoryProvider.getRepositoryManager(repo_path);
        manager.init();
        manager.getAllRepositories();
        
        return manager;

    }
}
