## Sobre o Plugin

O plugin Load Triple File tem como objetivo realizar o upload (carregamento) de arquivos triplificados em um banco de dados em Grafos, mais especificamente o [GraphDB](https://graphdb.ontotext.com/), tendo suporte a todos os formatos de arquivos disponíveis no mesmo, .ttl, .ttls, .rdf, .rj, .n3, .nt, .nq, .trig, .trigs, .trix, .owl, .jsonld. 

## Desenvolvimento

### Pré-requisitos

* [Pentaho Data Integration](https://sourceforge.net/projects/pentaho/)

Este projeto tem as seguintes dependências:

* [Java 8](https://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html) para desenvolvimento.
* [Maven](https://maven.apache.org/) para gestão das dependências.
* [Kettle 8.1+](https://sourceforge.net/projects/pentaho/) para testes e deploy.

Todas as dependências estão na pasta "dependency" para instalação.

Este projeto foi desenvolvido na IDE Eclipse, porém é agnóstico em relação a IDEs - não é necessário o uso do Eclipse para desenvolver neste projeto.

### Instalando

Para rodar o projeto em sua máquina, instale todos os pré-requisitos (cuidado especial na versão do java, que tem que ser Java 8), mude a variável ``pdi.home``, que representa o caminho referente a sua instalação do Pentaho Data Integration, no pom do projeto pai ``ETL4LODPLUSFAIR\plugins\pom.xml`` - <pdi.home>Caminho\data-integration</pdi.home>. Após isso, rode o comando ``mvn clean install`` dentro do pasta ``plugins`` (projeto pai). Isso instalará os plugins no Kettle especificado em ``pdi.home``.

Abra o Kettle 8.1 e uma pasta chamada LinkedDataBR deve aparecer com os plugins deste projeto. 

### Deployment

Para fazer deploy, rode na pasta do projeto pai:

``mvn clean install``

Tendo certeza de apontar a variável ``pdi.home`` para a sua instalação do Kettle 8.1.

## Contribuição

Para mais informações sobre o desenvolvimento, por favor, leia [CONTRIBUTING.md](CONTRIBUTING.md).

## Versionamento

Usamos [SemVer](http://semver.org/) para versionamento do ETL4LOD+. Para ver as versões liberadas, entre nas [tags deste repositorio](https://github.com/johncurcio/ETL4LODPlus/tags).

## Licença

Este projeto usa a licença do MIT, veja [LICENSE.md](LICENSE) para mais detalhes.

## Inspirado em

* [ETL4LOD](https://github.com/rogersmendonca/ETL4LOD),  [ETL4LOD-graph](https://github.com/rogersmendonca/ETL4LOD-Graph) e [ETL4LOD+](https://github.com/johncurcio/ETL4LODPlus/)
