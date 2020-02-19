# FBase

This project is part of the FBase research project which includes the following subprojects:
* [FBase](https://github.com/OpenFogStack/FBase): Main repository with the FBase system
* [FBaseNamingService](https://github.com/OpenFogStack/FBaseNamingService): The FBase Naming Service
* [FBaseCommons](https://github.com/OpenFogStack/FBaseCommons): Common utility classes used by FBase and the FBase Naming Service
* [FBaseExample](https://github.com/OpenFogStack/FBaseExample): Example FBase setup that uses Vagrant and VirtualBox

The combination of edge and cloud in the fog computing paradigm enables a new breed of data-intensive applications. These applications, however, have to face a number of fog-specific challenges which developers have to repetitively address for every single application.
FBase is a replication service specifically tailored to the needs of data-intensive fog applications that aims to ease or eliminate challenges caused by the highly distributed and heterogeneous environment fog applications operate in.

If you use this software in a publication, please cite it as:

### Text
Jonathan Hasenburg, Martin Grambow, David Bermbach. **Towards A Replication Service for Data-Intensive Fog Applications**. In: Proceedings of the 35th ACM Symposium on Applied Computing, Posters Track (SAC 2020). ACM 2020.

Jonathan Hasenburg, Martin Grambow, David Bermbach. **FBase: A Replication Service for Data-Intensive Fog Applications**. In: Technical Report MCC.2019.1. TU Berlin & ECDF, Mobile Cloud Computing Research Group. 2019.

### BibTeX
```
@inproceedings{paper_hasenburg_towards_fbase,
	title = {{Towards A Replication Service for Data-Intensive Fog Applications}},
	booktitle = {Proceedings of the 35th ACM Symposium on Applied Computing, Posters Track (SAC 2020)},
	publisher = {ACM},
	author = {Jonathan Hasenburg and Martin Grambow and David Bermbach},
	year = {2020}
}

@inproceedings{techreport_hasenburg_2019,
	title = {{FBase: A Replication Service for Data-Intensive Fog Applications}},
	booktitle = {Technical Report MCC.2019.1},
	publisher = {{TU Berlin \& ECDF, Mobile Cloud Computing Research Group}},
	author = {Jonathan Hasenburg and Martin Grambow and David Bermbach},
	year = {2019}
}
```

A full list of our [publications](https://www.mcc.tu-berlin.de/menue/forschung/publikationen/parameter/en/) and [prototypes](https://www.mcc.tu-berlin.de/menue/forschung/prototypes/parameter/en/) is available on our group website.

## Instructions

FBase can be build by running `mvn clean package`. Do not forget to run `mvn clean install` in the local FBaseCommons [repository](https://github.com/OpenFogStack/FBaseCommons) as it is one of the dependencies. In addition, the [Naming Service](https://github.com/OpenFogStack/FBaseNamingService) should be up and running (with the quickstart configuration).

The easiest way to startup FBase is by just using the jar file (with dependencies) produced by maven. If no argument is provided, a quickstart configuration is used. Otherwise, the only argument that can be provided is the path to a config file (that is then used instead of the quickstart config). For an example config file check out `src/main/resources/sample_config.properties.`, make sure that the initialNodeConfig of the naming service is compatible to the configuration of the first FBase node started (nodeID and encryption information), because otherwise it cannot communicate with the Naming Service. The quickstart configurations are compatible.

If you need to generate a private/public RSA key, use the `RSAHelper` class' main method.

## S3 Connector

If the S3 connector is supposed to be used, AWS credentials must be set locally as described [here](http://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/setup-credentials.html).

## How to run the tests

For some of the tests, a running naming service is required. The naming service has to be started in debug mode and must add an initial node that equals us as configured in the FBase configuration files. The naming service address must configured in the different configuration files for each test individually.
