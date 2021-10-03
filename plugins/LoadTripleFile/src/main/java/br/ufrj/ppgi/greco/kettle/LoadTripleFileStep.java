package br.ufrj.ppgi.greco.kettle;

import java.io.*;
import java.net.URL;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleValueException;
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


import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.model.util.Models;
import org.eclipse.rdf4j.model.vocabulary.RDF;

import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.config.RepositoryConfig;
import org.eclipse.rdf4j.repository.config.RepositoryConfigSchema;
import org.eclipse.rdf4j.repository.manager.RepositoryManager;
import org.eclipse.rdf4j.repository.manager.RepositoryProvider;
import org.eclipse.rdf4j.rio.*;

import org.eclipse.rdf4j.rio.helpers.StatementCollector;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.RDFParseException;


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

			try{

				// Se conecta ao Branco de Grafo
				RepositoryManager manager = InitRepo(inputRepoURL);
				System.out.println("Manager " + manager);

				// Obtendo e se conectando ao repositorio especifico do banco
				String repo_name = CreateRepo(manager, inputExistsRepository, inputRepoName);
				Repository repository = manager.getRepository(repo_name);
				RepositoryConnection repoCon = ConnectRepo(repository);
				System.out.println("Me conectei ao repositorio " + repo_name);

				/////////////////////////// Recupera arquivo
				File file = RetrieveFile(browseFilename);
				System.out.println("Estou com o arquivo " + file);

				/////////////////////////// Cria Grafo nomeado
				IRI context = ConnectRepoAndCreateNamedGraph(repoCon, inputGraphName);
				System.out.println("Criei o grafo " + inputGraphName);

				/////////////////////////// Carrega arquivo
				uploadFile(repoCon, file, context, inputFileFormat);
				System.out.println("Arquivo carregado!");

			} catch (IOException e) {
				e.printStackTrace();
            }

			

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
	public static RepositoryManager InitRepo(String repo_path) {

        String repo_url = repo_path;
        RepositoryManager manager = RepositoryProvider.getRepositoryManager(repo_url);
        manager.init();
        manager.getAllRepositories();
		System.out.println("Repo iniciado! " + manager);
        
        return manager;

    }

	// Cria repositório
	public static String CreateRepo(RepositoryManager manager, String inputExistsRepository, String inputRepoName) throws IOException {


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
            Model graph_test = new LinkedHashModel();

            try {

				config_test = new FileInputStream(new File(System.getProperty("user.dir") + "\\plugins\\steps\\LoadTripleFile\\lib\\repo-defaults_test.ttl"));
				System.out.println("Peguei config teste");

            } catch (FileNotFoundException e) {				
                e.printStackTrace();
            }

            rdfParser_test = Rio.createParser(RDFFormat.TURTLE);
           	rdfParser_test.setRDFHandler(new StatementCollector(graph_test));
			System.out.println("Fiz o parser");


			try {

                rdfParser_test.parse(config_test, RepositoryConfigSchema.NAMESPACE);
				System.out.println("Fiz o parser2");
            }
            catch (IOException e) {
                e.printStackTrace();
            }
            finally {

                config_test.close(); }

            //Obtendo o repositório como recurso
            Resource repoNode_test = Models.subject(graph_test.filter(null, RDF.TYPE, RepositoryConfigSchema.REPOSITORY)).orElseThrow(() -> new RuntimeException("Oops, no <http://www.openrdf.org/config/repository#> subject found!"));
			System.out.println("Fiz o repoNode_test");

            //Adicionando as configurações
            RepositoryConfig configObj = RepositoryConfig.create(graph_test, repoNode_test);
			System.out.println("Fiz o configObj");
            manager.addRepositoryConfig(configObj);

            repo_name = "repo_pdi";

        }

        System.out.println("Repo Criado!");
		return repo_name;

    }

	public static File RetrieveFile(String browseFilename){

        // Recupera arquivo e cria objeto File
		File file = new File(browseFilename); 

        return file;

    }

	public static RepositoryConnection ConnectRepo(Repository repository){

        //Conectar ao repositorio
        RepositoryConnection repoCon = repository.getConnection();

        return repoCon;

    }

	public static IRI ConnectRepoAndCreateNamedGraph(RepositoryConnection repoConnection, String inputGraphName){
			
        String ex = "http://etl4lod.com/";
        ValueFactory vf = repoConnection.getValueFactory();
        IRI context = vf.createIRI(ex, inputGraphName); // Cria contexto para novo grafo nomeado

        return context;

    }

	public static void uploadFile(RepositoryConnection repoConnection, File file_Path, IRI context, String inputFileFormat){


        // Base URI
        String baseURI = "http://example.org/example/local";

        switch (inputFileFormat){

            // Arquivos .ttl
            case "TURTLE":
                try {

                    repoConnection.add(file_Path, baseURI, RDFFormat.TURTLE, context);

                } catch (IOException e) {

                    e.printStackTrace();

                }
                System.out.println("Upload Finalizado");
                break;

            // Arquivos .rdf, .rdfs, .owl, .xml
            case "RDFXML":
                try {

                    repoConnection.add(file_Path, baseURI, RDFFormat.RDFXML, context);

                } catch (IOException e) {

                    e.printStackTrace();

                }
                System.out.println("Upload Finalizado");
                break;

            // Arquivos .rj
            case "RDFJSON":
                try {

                    repoConnection.add(file_Path, baseURI, RDFFormat.RDFJSON, context);

                } catch (IOException e) {

                    e.printStackTrace();

                }
                System.out.println("Upload Finalizado");
                break;

            // Arquivos .n3
            case "N3":
                try {

                    repoConnection.add(file_Path, baseURI, RDFFormat.N3, context);

                } catch (IOException e) {

                    e.printStackTrace();

                }
                System.out.println("Upload Finalizado");
                break;

            // Arquivos .nt
            case "NTRIPLES":
                try {

                    repoConnection.add(file_Path, baseURI, RDFFormat.NTRIPLES, context);

                } catch (IOException e) {

                    e.printStackTrace();

                }
                System.out.println("Upload Finalizado");
                break;

            // Arquivos .nq
            case "NQUAD":
                try {

                    repoConnection.add(file_Path, baseURI, RDFFormat.NQUADS, context);

                } catch (IOException e) {

                    e.printStackTrace();

                }
                System.out.println("Upload Finalizado");
                break;

            // Arquivos .trig
            case "TRIG":
                try {

                    repoConnection.add(file_Path, baseURI, RDFFormat.TRIG, context);

                } catch (IOException e) {

                    e.printStackTrace();

                }
                System.out.println("Upload Finalizado");
                break;

            // Arquivos .trix
            case "TRIX":
                try {

                    repoConnection.add(file_Path, baseURI, RDFFormat.TRIX, context);

                } catch (IOException e) {

                    e.printStackTrace();

                }
                System.out.println("Upload Finalizado");
                break;

            // Arquivos .jsonld
            case "JSONLD":
                try {

                    repoConnection.add(file_Path, baseURI, RDFFormat.JSONLD,context);

                } catch (IOException e) {

                    e.printStackTrace();

                }
                System.out.println("Upload Finalizado");
                break;

        }

    }
}
