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
import org.eclipse.rdf4j.repository.config.RepositoryConfig;
import org.eclipse.rdf4j.repository.config.RepositoryConfigSchema;
import org.eclipse.rdf4j.repository.http.HTTPGraphQuery;
import org.eclipse.rdf4j.repository.http.HTTPRepository;
import org.eclipse.rdf4j.repository.manager.RepositoryManager;
import org.eclipse.rdf4j.repository.manager.RepositoryProvider;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.StatementCollector;
import org.eclipse.rdf4j.sparqlbuilder.core.query.LoadQuery;
import org.eclipse.rdf4j.sparqlbuilder.core.query.Queries;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.util.UriEncoder;


import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;

import static org.eclipse.rdf4j.rio.RDFFormat.NTRIPLES;
import static org.eclipse.rdf4j.rio.RDFFormat.RDFXML;


public class QuerySPARQL<iterator> {

    private static Logger logger = LoggerFactory.getLogger(QuerySPARQL.class);
    static TupleQuery query;
    static File saida = new File("saida.txt");
    static String httpRepo = new String("http://192.168.0.6:7200/");
    static String repoID = new String("T0");
    static RepositoryConnection con;
    static BindingSet binding;
    static LocalBase locB;//Base local na memória
    static ArrayList<String> graphAdds = new ArrayList<String>();
    static ArrayList<String> extraQueries = new ArrayList<String>();
    static int numGraphs;
    static int currGraph;
    static int numExtQueires=0;
    static String tempGraphName=null;
    static ArrayList<String> tempCurGraphs = new ArrayList<String>();
    static String curGraphQueryURI = new String();
    //Teste de carregamento
    static RepositoryConnection repoCon_test=null;

    private QuerySPARQL(){

    }


    public static void main(String args[]) {
        //inicializa o repositório




        String results = new String();
        String[] queries, values, classes;
        Boolean primeira=true;
        int count=0;
        int current = 1;
        int numQueries = 0;
        int startQueryNumber;
        int queryProcessing=2;
        Iterator inter;




        //Carrega as consultas do arquivo

        String fileAsString = dadosQuery();
        queries=fileAsString.split("#QUERY");//separador das consultas
        numQueries=queries.length;
        System.out.println(numQueries);
        startQueryNumber=numQueries;
        locB = new LocalBase();
        currGraph=0;
        numGraphs=0;

        try {
            load_data();
        } catch (IOException e) {
            e.printStackTrace();
        }

        while(current < numQueries) {
            count=0;
            primeira=true;
            //System.out.println("\n\n********\n\nComeçou\n\n********\n\n");
            iniciaRepo();
            //Realiza a consulta



            while(current>1&&currGraph<numGraphs){

                if(!(current >1)) {
                    tempGraphName =  new String(graphAdds.get(currGraph));

                    queries[current] = queries[current].replaceAll("#graphToQuery",  tempGraphName + "");
                }
                else{
                    tempGraphName =  new String(graphAdds.get(currGraph));
                    tempCurGraphs.add(tempGraphName);
                    extraQueries.add(queries[queryProcessing].replaceAll("#graphToQuery", tempGraphName + "/"));
                    //numQueries++;
                    System.out.println(queries[queryProcessing].replaceAll("#graphToQuery", tempGraphName + "/"));

                }
                //System.out.println(queries[current]);
                //System.out.println("\n ***"+current+"***** \n");
                currGraph++;

            }

            if(current==2){
                numQueries=(startQueryNumber-2)*(numGraphs)+1;
                System.out.println("\n\n"+numQueries+"\n\n");
                //geraQueries(current,queries);
            }


            if(current <=1){
                query = con.prepareTupleQuery(QueryLanguage.SPARQL, queries[current]);
            }
            if(current>1&&current<numQueries){
                query = con.prepareTupleQuery(QueryLanguage.SPARQL, extraQueries.get(current-2).toString());
                System.out.println("\n\n EXECUÇÂO \n\n");
                System.out.println(extraQueries.get(current-2).toString());
                for (String element : tempCurGraphs){
                    //System.out.println("\n"+element+"\n");
                    if(extraQueries.get(current-2).toString().contains((CharSequence) element)){
                        curGraphQueryURI=element;
                        //System.out.println("\n\nENCONTROU BASE"+curGraphQueryURI+"\n\n");
                    }
                }
                if(current-2==extraQueries.size()-1){
                    currGraph=0;
                    queryProcessing++;
                }

            }


            TupleQueryResult result = null;

            result = query.evaluate();
            values = new String[result.getBindingNames().size()];
            classes = new String[result.getBindingNames().size()];

            ModelBuilder modBuilder = new ModelBuilder();
            ValueFactory factory = null;
            ValueFactory factoryLoc = null;

            //cria repositório

            InputStream config = null;
            RDFParser rdfParser = null;
            TreeModel graph = new TreeModel();
           // Model model = graph.filter(null, RDF.TYPE, RepositoryConfigSchema.REPOSITORY);


            RepositoryManager manager = RepositoryProvider.getRepositoryManager("http://192.168.0.6:7200");
            manager.init();
            manager.getAllRepositories();

            try {

                config = new FileInputStream(new File("repo-defaults.ttl"));
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
            rdfParser = Rio.createParser(RDFFormat.TURTLE);
            rdfParser.setRDFHandler(new StatementCollector(graph));

            try {
                rdfParser.parse(config, RepositoryConfigSchema.NAMESPACE);
            } catch (IOException e) {
                e.printStackTrace();
            }


            try {
                config.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            //


            //Obtendo o repositório como recurso
            Resource repoNode = Models.subject(graph.filter(null, RDF.TYPE, RepositoryConfigSchema.REPOSITORY)).orElseThrow(() -> new RuntimeException("Oops, no <http://www.openrdf.org/config/repository#> subject found!"));


            //Adicionando as configurações
            RepositoryConfig configObj = RepositoryConfig.create(graph, repoNode);
            manager.addRepositoryConfig(configObj);


            //Obter o repositorio criado
            Repository repository = manager.getRepository("graphdb-repo");

            //Conectar ao repositorio
            RepositoryConnection repoCon = repository.getConnection();
            factory= repoCon.getValueFactory();//inicializa o value factory correspondente ao respositório criado
            factoryLoc=locB.getConnection().getValueFactory();


            //Fim dos processos de criação do repositório

            if (primeira) {

                while (count < result.getBindingNames().size()) {
                    //System.out.println(result.getBindingNames().get(0));
                    values[count] = String.valueOf(result.getBindingNames().get(count));
                    results += values[count] + "\t";
                    count++;
                }
                results += "\n";
                primeira = false;

            }

            System.out.println(result.getBindingNames());
            while (result.hasNext()) {//Avança por cada linha dos resultados
                binding = result.next();
                //System.out.println(binding.size());

                count = 0;
                while (count < binding.size()) {
                    //System.out.println(binding.getValue(values[count]).toString());
                    classes[count] = binding.getValue(values[count]).toString();
                    count++;
                }

                //modBuilder.subject(binding.getValue("DS").stringValue())
                //Criando as triplas. Teste. Restringindo a cada caso

                triplifica(repoCon, factory, factoryLoc, current);


                //if(binding.hasBinding(""))

                results+=tempGraphName+"\n";
                //results+=graphAdds.get(currGraph).toString();
                for (String clas : classes) {
                    results += clas + "\t";
                }
                results += "\n";
                /*
                try {
                    FileUtils.writeStringToFile(saida,results);
                } catch (IOException e) {
                    e.printStackTrace();
                }*/


            }
            curGraphQueryURI="";//Esvazia a URI para que a nova possa ser obtida e testada
            con.close();
            escreveArquivo(results);





            //        //Carregar dados
            //        repoCon.begin();
            //        Update updateOp = repoCon.prepareUpdate(QueryLanguage.SPARQL, classeConceito.stringValue());
            //        updateOp.execute();


            //carregaDados(repoCon);




            //Encerrar a conexão
            repoCon.close();
            repository.shutDown();
            manager.shutDown();


            //        RepositoryConfig repositoryConfig = RepositoryConfig.create(graph,repositoryNode);
            //        manager.addRepositoryConfig(repositoryConfig);
            //

            current++;
        }
        //locB.runQuery("select * where { ?s ?p ?o. }");
        //locB.printQueryResult();


        //locB.finishConnection();


    }

    private static int iniciaRepo(){
        Repository repo = new HTTPRepository(httpRepo, repoID);
        repo.init();
        con = repo.getConnection();
        return 0;
    }

    private static String dadosQuery(){//Obtém os dados de quey do arquivo
        InputStream stream = null;
        String line=null;
        BufferedReader buf = null;
        //Carrega as consultas do arquivo
        try {
            stream = new FileInputStream(new File("queries.txt"));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        buf = new BufferedReader(new InputStreamReader(stream));
        try {
            line = buf.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
        //Constroi a string com o conteúdo do arquivo
        StringBuilder sb = new StringBuilder();



        while(line != null){
            sb.append(line).append("\n");
            try {
                line = buf.readLine();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        String fileAsString = sb.toString();
        return sb.toString();
    }

    private static String dadosTriplas(){//Obtém os dados de quey do arquivo
        InputStream stream = null;
        String line=null;
        BufferedReader buf = null;
        //Carrega as consultas do arquivo
        try {
            stream = new FileInputStream(new File("triple_data/load_list.txt"));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        buf = new BufferedReader(new InputStreamReader(stream));
        try {
            line = buf.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
        //Constroi a string com o conteúdo do arquivo
        StringBuilder sb = new StringBuilder();


        while(line != null){
            sb.append(line).append("\n");
            try {
                line = buf.readLine();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        String fileAsString = sb.toString();
        return sb.toString();
    }

    private static int escreveArquivo(String cont){
        try{
            FileUtils.writeStringToFile(saida,cont);
        } catch (IOException e) {
            e.printStackTrace();
            return 1;
        }
        return 0;
    }

    private static int carregaDados(RepositoryConnection repoCon){



        try {
            repoCon.add(new File("metadata_from_portal.rdf"),"", RDFXML);
        } catch (IOException e) {
            e.printStackTrace();
        }


        return 0;
    }

    private static int triplifica(RepositoryConnection repoCon, ValueFactory factory, ValueFactory factoryLoc, int current){

        IRI grafoClasses = factory.createIRI("http://example.org/GrafoClasses/");
        if(binding.hasBinding("DS")&&binding.hasBinding("nomeOrg")) {
            repoCon.add(factory.createIRI("http://"+binding.getValue("nomeOrg").stringValue().replaceAll("[, ]","_")), RDF.TYPE, FOAF.ORGANIZATION, grafoClasses);
            repoCon.add(factory.createIRI("http://"+binding.getValue("nomeOrg").stringValue().replaceAll("[, ]","_")), FOAF.NAME, factory.createLiteral(binding.getValue("titleDistrib").stringValue()), grafoClasses);
            repoCon.add(factory.createIRI(binding.getValue("DS").stringValue()), RDF.TYPE, DCAT.DATASET,grafoClasses);
            repoCon.add(factory.createIRI(binding.getValue("DS").stringValue()), DC.PUBLISHER, factory.createIRI("http://"+binding.getValue("nomeOrg").stringValue().replaceAll("[, ]","_")),grafoClasses);
            repoCon.add(factory.createIRI(binding.getValue("idDistrib").stringValue()), RDF.TYPE, DCAT.DISTRIBUTION,grafoClasses);
            repoCon.add(factory.createIRI(binding.getValue("DS").stringValue()), DC.TITLE, factory.createLiteral(binding.getValue("titleDS").stringValue()),grafoClasses);
            repoCon.add(factory.createIRI(binding.getValue("idDistrib").stringValue()),DCAT.HAS_DISTRIBUTION,factory.createIRI(binding.getValue("DS").stringValue()),grafoClasses);
            //repoCon.add(factory.createIRI(binding.getValue("DS").stringValue()), factory.createIRI("http://purl.org/dc/terms/references"), factory.createIRI(binding.getValue("nomeOrg").stringValue()));
            graphAdds.add((binding.getValue("idDistrib").stringValue().replaceAll("\"","")));//adiciona as URIs dos grafos
            //System.out.println("\n"+binding.getValue("idDistrib").stringValue().replaceAll("\"","")+"\n");
            if(current==1){
                numGraphs++;//Define o número de grafos a partir da consulta inicial
            }

            //System.out.println("\nNUM GRAPHS"+numGraphs);
        }
        /*if(binding.hasBinding("DS")&&binding.hasBinding("nomeOrg")) {
            locB.getConnection().add(factoryLoc.createIRI(binding.getValue("nomeOrg").stringValue()), RDF.TYPE, factory.createIRI(binding.getValue("publicador").stringValue()));
            locB.getConnection().add(factoryLoc.createIRI(binding.getValue("DS").stringValue()), RDF.TYPE, DCAT.DATASET);
            locB.getConnection().add(factoryLoc.createIRI(binding.getValue("DS").stringValue()), DC.PUBLISHER, factory.createIRI(binding.getValue("nomeOrg").stringValue()));
            locB.getConnection().add(factoryLoc.createIRI(binding.getValue("DS").stringValue()), factory.createIRI("http://purl.org/dc/terms/references"), factory.createIRI(binding.getValue("nomeOrg").stringValue()));
        }*/
        if(binding.hasBinding("ClasseConceito1")&&binding.hasBinding("ClasseConceito2")&&(!curGraphQueryURI.equals(""))){
            repoCon.add(factory.createIRI(curGraphQueryURI.toString()), factory.createIRI("http://purl.org/dc/terms/references"), factory.createIRI(binding.getValue("ClasseConceito1").stringValue()),grafoClasses);
            repoCon.add(factory.createIRI(curGraphQueryURI.toString()), factory.createIRI("http://purl.org/dc/terms/references"), factory.createIRI(binding.getValue("ClasseConceito1").stringValue().substring(0,binding.getValue("ClasseConceito1").stringValue().lastIndexOf("/"))),grafoClasses);
            System.out.println(binding.getValue("\n\n"+"ClasseConceito1"));
        }
        if(binding.hasBinding("ClasseConceito")&&binding.hasBinding("ClasseConceitoLabel")&&(!curGraphQueryURI.equals(""))){
            repoCon.add(factory.createIRI(curGraphQueryURI.toString()), factory.createIRI("http://purl.org/dc/terms/references"), factory.createIRI(binding.getValue("ClasseConceito").stringValue()),grafoClasses);
            repoCon.add(factory.createIRI(curGraphQueryURI.toString()), factory.createIRI("http://purl.org/dc/terms/references"), factory.createIRI(binding.getValue("ClasseConceito").stringValue().substring(0,binding.getValue("ClasseConceito").stringValue().lastIndexOf("/"))),grafoClasses);

        }

        return 0;
    }
    private static int load_data()throws IOException{
        Update lquery = null;
        File triplas;
        String fileAsString_test = dadosTriplas();
        ValueFactory factory_test = null;
        InputStream config_test = null;
        RDFParser rdfParser_test = null;
        TreeModel graph_test = new TreeModel();
        String[] prefix=fileAsString_test.split("(.*)#URL");//separador das consultas
        String[] arquivos=fileAsString_test.split("#URL(.*)");
        int index=0;
        //ModelBuilder mBuilder = new ModelBuilder();


       // Model model_test = graph_test.filter(null, RDF.TYPE, RepositoryConfigSchema.REPOSITORY);


        RepositoryManager manager_test = RepositoryProvider.getRepositoryManager("http://192.168.0.6:7200");
        manager_test.init();
        manager_test.getAllRepositories();

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
        //


        //Obtendo o repositório como recurso
        Resource repoNode_test = Models.subject(graph_test.filter(null, RDF.TYPE, RepositoryConfigSchema.REPOSITORY)).orElseThrow(() -> new RuntimeException("Oops, no <http://www.openrdf.org/config/repository#> subject found!"));


        //Adicionando as configurações
        RepositoryConfig configObj = RepositoryConfig.create(graph_test, repoNode_test);
        manager_test.addRepositoryConfig(configObj);


        //Obter o repositorio criado
        Repository repository_test = manager_test.getRepository("graphdb-repo-test");

        //Conectar ao repositorio
        repoCon_test = repository_test.getConnection();

        while (index<arquivos.length-1) {
            //System.out.println(index+"\n"+arquivos.length);
            triplas =  new File("triple_data/"+arquivos[index].replaceAll("\n",""));
            //System.out.println("/n"+triplas.getAbsolutePath().replaceAll("\\\\","/"));
            lquery=repoCon_test.prepareUpdate(QueryLanguage.SPARQL, "load <file:///"+triplas.getAbsolutePath().replaceAll("\\\\","/")+"> into graph <"+prefix[index+1].replaceAll("\n","")+">");
            //loadQuery= repoCon_test.prepareTupleQuery("load <file:///"+triplas.getAbsolutePath().replaceAll("\\\\","/")+"> into graph <"+prefix[index]+">");
            //System.out.println("load <file:///"+triplas.getAbsolutePath().replaceAll("\\\\","/")+"> into graph <"+prefix[index+1].replaceAll("\n","")+">");
            lquery.execute();
            index++;
            //TODO Deletar os dados do repositorio
        }


        return 0;

    }

    private static int geraQueries(int current,String[] queries){
        int queryNumber=0;
        while(current>1&&queryNumber<numExtQueires){
            while(currGraph<numGraphs){

                tempGraphName =  new String(graphAdds.get(currGraph));
                tempCurGraphs.add(tempGraphName);
                extraQueries.add(queries[current].replaceAll("#graphToQuery", tempGraphName + ""));
                //numQueries++;
                System.out.println(queries[current].replaceAll("#graphToQuery", tempGraphName + ""));


                //System.out.println(queries[current]);
                //System.out.println("\n ***"+current+"***** \n");
                currGraph++;

            }
        }
        return 0;
    }



    // Instantiate a repository graph model


//    // Read repository configuration file
//    InputStream config;
//
//    {
//        try {
//            config = new FileInputStream(new File("repo-defaults.ttl"));
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        }
//    }
//




}
