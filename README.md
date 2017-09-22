# Configuration

If the S3 connector is supposed to be used, AWS credentials must be set locally as described [here](http://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/setup-credentials.html).

# How to run the tests

For some of the tests, a running naming service is required. The naming service has to be started in debug mode and must add an initial node that equals us as configured in the FBase configuration files. The naming service address must configured in the different configuration files for each test individually.

# Current ToDos / Missing Functionality

## Startup
- [ ] A machineName should be dynamically created on Startup (clean up configs)
- [x] Run AddMachineToNodeTask (ended up not being a task)

## Subscription Management incl. Heartbeats
- See ImplementationSubscriptionManagement.md

## One to One Communication
- [ ] Add one to one communication for datarecords

### Handling Missed Messages
- [ ] Add message history size to machine config (node specific), cleanup config files
- [ ] (Add message history cleanup functionality (on receiver and sender side)), thesis states not implemented

## Controlling FBase with Clients
 - [ ] Enable encryption and authentication
 - [ ] Method to instruct machine to update all configurations with naming service data
 - [x] Keygroup C, R, D
 - [x] Keygroup all update methods
 - [X] Client C, D
 - [X] DataRecord P, R, D, List
