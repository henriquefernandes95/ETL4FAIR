/*
Projeto de suporte a triplificação
Idealised by: Gláucia Botelho de Figueiredo
Coded by: Henrique Fernandes Rodrigues
Started at: 18/03/2020
Java 11.0.7
IntelliJ IDEA 2020.1
*/


import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.server.persistence.Util;
import org.eclipse.rdf4j.common.lang.FileFormat;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.impl.SimpleIRI;
import org.eclipse.rdf4j.model.impl.TreeModel;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.model.util.Models;
import org.eclipse.rdf4j.model.util.URIUtil;
import org.eclipse.rdf4j.model.vocabulary.DC;
import org.eclipse.rdf4j.model.vocabulary.DCAT;
import org.eclipse.rdf4j.model.vocabulary.FOAF;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.query.*;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryResult;
import org.eclipse.rdf4j.repository.config.RepositoryConfig;
import org.eclipse.rdf4j.repository.config.RepositoryConfigSchema;
import org.eclipse.rdf4j.repository.http.HTTPGraphQuery;
import org.eclipse.rdf4j.repository.http.HTTPRepository;
import org.eclipse.rdf4j.repository.manager.RepositoryManager;
import org.eclipse.rdf4j.repository.manager.RepositoryProvider;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandler;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.StatementCollector;
import org.eclipse.rdf4j.sparqlbuilder.core.query.LoadQuery;
import org.eclipse.rdf4j.sparqlbuilder.core.query.Queries;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.util.UriEncoder;


import java.io.*;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Scanner;

import static org.eclipse.rdf4j.rio.RDFFormat.NTRIPLES;
import static org.eclipse.rdf4j.rio.RDFFormat.RDFXML;


public class PrimeiraClasse<iterator> {

    public static void main(String args[]) {

        // Recupera variaveis do teclado
        Scanner keyboard = new Scanner(System.in);

        //inicializa o repositório
        RepositoryManager manager = RepositoryProvider.getRepositoryManager("http://192.168.0.6:7200");
        manager.init();
        manager.getAllRepositories();

        // Verifica se vai usar repositorio ja existente ou criar um novo
        System.out.println("Usar repositorio existente?(S/N) Se nao, criara um novo com o nome 'repo_pdi'");
        String existis_repo = keyboard.next();
        String repo_name = null;

        // Repositorio ja existe
        if ( existis_repo.equals("S") ){

            System.out.println("Qual o nome do repositorio?");
            repo_name = keyboard.next();
            
        } else if( existis_repo.equals("N") ){
            InputStream config_test = null;
            RDFParser rdfParser_test = null;
            TreeModel graph_test = new TreeModel();

            try {

                config_test = new FileInputStream(new File("triple_data/repo-defaults_test.ttl"));
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
            rdfParser_test = Rio.createParser(RDFFormat.TURTLE);
            rdfParser_test.setRDFHandler(new StatementCollector(graph_test));

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

        //Obter o repositorio criado
        Repository repository = manager.getRepository(repo_name);

        //Conectar ao repositorio
        RepositoryConnection repoCon = repository.getConnection();
        ValueFactory vf = repoCon.getValueFactory();

        /////////////////////////// Recupera arquivo
        System.out.println("Entre o caminho absoluto do arquivo");
        String file_path = keyboard.next();
        System.out.println("O nome do grafo eh: " + file_path);
        File file = new File(file_path);
        /////////////////////////// Grafo nomeado --> Passar nome como IRI
        System.out.println("Entre a IRI do Grafo - http://example.org/example/example.rdf");
        String graph_name = keyboard.next();
        System.out.println("O nome do grafo eh: " + graph_name);
        String location = graph_name;
        String baseURI = location;
        IRI context = vf.createIRI(location);
        /////////////////////////// Formato do arquivo que deve carregar
        System.out.println("Entre o formato do arquivo - RDFXML, N3");
        String file_format = keyboard.next();
        String file_format2 = file_format;
        System.out.println("O formato do arquivo eh: " + file_format2);







        switch (file_format){

            // Arquivos .ttl
            case "TURTLE":
                try {

                    repoCon.add(file, baseURI, RDFFormat.TURTLE, context);

                } catch (IOException e) {

                    e.printStackTrace();

                }
                System.out.println("Fechei o case");
                break;

            // Arquivos .rdf, .rdfs, .owl, .xml
            case "RDFXML":
                try {

                    repoCon.add(file, baseURI, RDFFormat.RDFXML, context);

                } catch (IOException e) {

                    e.printStackTrace();

                }
                System.out.println("Fechei o case");
                break;

            // Arquivos .rj
            case "RDFJSON":
                try {

                    repoCon.add(file, baseURI, RDFFormat.RDFJSON, context);

                } catch (IOException e) {

                    e.printStackTrace();

                }
                System.out.println("Fechei o case");
                break;

            // Arquivos .n3
            case "N3":
                try {

                    repoCon.add(file, baseURI, RDFFormat.N3, context);

                } catch (IOException e) {

                    e.printStackTrace();

                }
                System.out.println("Fechei o case");
                break;

            // Arquivos .nt
            case "NTRIPLES":
                try {

                    repoCon.add(file, baseURI, RDFFormat.NTRIPLES, context);

                } catch (IOException e) {

                    e.printStackTrace();

                }
                System.out.println("Fechei o case");
                break;

            // Arquivos .nq
            case "NQUAD":
                try {

                    repoCon.add(file, baseURI, RDFFormat.NQUADS, context);

                } catch (IOException e) {

                    e.printStackTrace();

                }
                System.out.println("Fechei o case");
                break;

            // Arquivos .trig
            case "TRIG":
                try {

                    repoCon.add(file, baseURI, RDFFormat.TRIG, context);

                } catch (IOException e) {

                    e.printStackTrace();

                }
                System.out.println("Fechei o case");
                break;

            // Arquivos .trix
            case "TRIX":
                try {

                    repoCon.add(file, baseURI, RDFFormat.TRIX, context);

                } catch (IOException e) {

                    e.printStackTrace();

                }
                System.out.println("Fechei o case");
                break;

            // Arquivos .jsonld
            case "JSONLD":
                try {

                    repoCon.add(file, baseURI, RDFFormat.JSONLD,context);

                } catch (IOException e) {

                    e.printStackTrace();

                }
                System.out.println("Fechei o case");
                break;

        }

//        if (file_format2.equals("RDFXML") ) {
//
//            System.out.println("Entrei aqui");
//            try {
//                repoCon.add(file, baseURI, RDFFormat.RDFXML, context);
//                //URL url = new URL("http://example.org/");
//                //repoCon.add(url, url.toString(), RDFFormat.RDFXML);
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//            // Get all statements in the context
//            try (RepositoryResult<Statement> result = repoCon.getStatements(null, null, null, context)) {
//                while (result.hasNext()) {
//                    Statement st = result.next();
//                    // do something interesting with the result
//                    System.out.println("Teste Nickolas" + st);
//                }
//            }
//            // Export all statements in the context to System.out, in RDF/XML format
//            RDFHandler writer = Rio.createWriter(RDFFormat.RDFXML, System.out);
//            repoCon.export(writer, context);
//            // Remove all statements in the context from the repository
//            //repoCon.clear(context);
//
//        }

        //Encerrar a conexão
        repoCon.close();
        repository.shutDown();
        manager.shutDown();

    }

}
