# FBase

FBase can be build by running `mvn clean package`. Do not forget to run `mvn clean install` in the local FBaseCommons [repository](https://github.com/OpenFogStack/FBaseCommons) as it is one of the dependencies. In addition, the [Naming Service](https://github.com/OpenFogStack/FBaseNamingService) should be up and running (with the quickstart configuration).

The easiest way to startup FBase is by just using the jar file (with dependencies) produced by maven. If no argument is provided, a quickstart configuration is used. Otherwise, the only argument that can be provided is the path to a config file (that is then used instead of the quickstart config). For an example config file check out `src/main/resources/sample_config.properties.`, make sure that the initialNodeConfig of the naming service is compatible to the configuration of the first FBase node started (nodeID and encryption information), because otherwise it cannot communicate with the Naming Service. The quickstart configurations are compatible.

If you need to generate a private/public RSA key, use the `RSAHelper` class' main method.

## Related Publications


Jonathan Hasenburg, Martin Grambow, David Bermbach. ***Towards A Replication Service for Data-Intensive Fog Applications.*** In: Proceedings of the 35th ACM Symposium on Applied Computing, Posters Track (SAC 2020). ACM 2020.

Jonathan Hasenburg, David Bermbach. ***Towards Geo-Context Aware IoT Data Distribution.*** In: Proceedings of the 4th Workshop on IoT Systems Provisioning and Management for Context-Aware Smart Cities (ISYCC 2019). Springer 2019.

## S3 Connector

If the S3 connector is supposed to be used, AWS credentials must be set locally as described [here](http://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/setup-credentials.html).

## How to run the tests

For some of the tests, a running naming service is required. The naming service has to be started in debug mode and must add an initial node that equals us as configured in the FBase configuration files. The naming service address must configured in the different configuration files for each test individually.

## Current ToDos not listed in code

### Storage
- [ ] Remove data that is expired.

### Handling Missed Messages
- [ ] Add message history cleanup functionality (on receiver and sender side). Currently, the message history is never cleaned.

### Controlling FBase with Clients
 - [ ] Enable encryption and authentication (Currently, everyone can use the rest API of a node and do everything. However, the node has to check whether the requesting client is already registered with the naming service.)
