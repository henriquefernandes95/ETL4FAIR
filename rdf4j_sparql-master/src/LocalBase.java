/*
Projeto de suporte a triplificação
Idealized by: Gláucia Botelho de Figueiredo
Coded by: Henrique Fernandes Rodrigues
Started at: 18/03/2020
Java 11.0.7
IntelliJ IDEA 2020.1
*/
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.sail.memory.MemoryStore;

import java.io.File;
import java.io.IOException;



import static org.eclipse.rdf4j.rio.RDFFormat.RDFXML;

public class LocalBase {
    private Repository localRepo;
    private RepositoryConnection repCon;
    private TupleQuery query;
    private TupleQueryResult queryResult;
    private BindingSet bSet;


    public LocalBase(){
        this.localRepo = new SailRepository(new MemoryStore());
        this.repCon = localRepo.getConnection();
        this.loadRDFData();
        localRepo.init();

    }
    public int loadRDFData(){
        try {
            repCon.add(new File("metadata_from_portal.rdf"),"", RDFXML);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 0;
    }
    public RepositoryConnection getConnection(){

        return repCon;
    }
    public void runQuery(String queryCode){

        query = repCon.prepareTupleQuery(QueryLanguage.SPARQL,queryCode);
        queryResult=query.evaluate();
        //return queryResult;
    }
    public void printQueryResult(){

        while(queryResult.hasNext()){
            bSet=queryResult.next();
            System.out.println(bSet.getValue(bSet.getBindingNames().toArray()[1].toString()) + "\t" + bSet.getValue(bSet.getBindingNames().toArray()[0].toString()) + "\t" + bSet.getValue(bSet.getBindingNames().toArray()[2].toString()) + "\n");
        }
    }
    public void finishConnection(){
        repCon.close();
    }
    public static void main(String args[]){
        /*LocalBase base = new LocalBase();
        base.runQuery("select * where { ?s ?p ?o. }limit 10");
        base.printQueryResult();*/



    }
}
