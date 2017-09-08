# Configuration

If the S3 connector is supposed to be used, AWS credentials must be set locally as described [here](http://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/setup-credentials.html).

# How to run the tests

For some of the tests, a running naming service is required. The naming service has to be started in debug mode and must add an initial node that equals us as configured in the FBase configuration files. The naming service address must configured in the different configuration files for each test individually.

# Current ToDos / Missing Functionality

## General Node Database
- [x] Add a connector that supports multi-machine nodes
- [x] Add versions to all configurations so that a node/machine can identify updates

## Publish/Subscribe
- [x] Add capabilities to process messages that cannot be encrypted

### Subscription Management
- [x] Add background task that checks whether any of the keygroups I am responsible for have been updated by another machine (CheckKeygroupConfigurationsOnUpdatesTask) #11
- [x] Instead of unsubscribing/subscribing, each keygroup config update should lead to a complete reset of subscriptions
- [ ] Add background task that checks whether any keygroups don't have a responsible machine yet #11

### Heartbeats
- [ ] Add background task that stores own heartbeats in the node database #11
- [ ] Add background task that checks other machine's heartbeats and removes machines from a node if they did not respond to long #11

## One to One Communication
- [x] Rebuild asymmetric encryption so that it uses a symmetric approach for the actual data

### Naming Service based Management
- [x] Add keygroup configuration control methods to sender
- [x] Add response processing to all methods
- [x] Add node management methods (e.g. to update a node configuration when a machine is added/removed)
- [ ] Add a background task that periodically polls the naming service about the newest configurations

### Handling Missed Messages
- [ ] Add message history size to machine config
- [ ] Messaging data needs to be stored after each data related publish
- [ ] Add sender/receiver capabilities (get requests)
- [ ] Write logic that uses functionality

## Controlling FBase with Clients
 - [ ] Enable asymmetric encryption
 - [ ] Identify and add missing control methods
