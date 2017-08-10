# Current ToDos / Missing Functionality

## General Node Database
- [ ] Make sure the HeapDBConnector only returns copies of objects rather than the actual objects
- [ ] Add a connector that supports multi-machine nodes

## Publish/Subscribe

### Subscription Management
- [ ] Add background task that checks whether any of the keygroups I am responsible for have been updated by another machine (CheckKeygroupConfigurationsOnUpdatesTask) #11
- [ ] Add background task that checks whether any keygroups don't have a responsible machine yet #11
- [ ] Add functionality to unsubscribe from machines if not responsible anymore or if keygroup has been deleted/node removed without the machine noticing

### Heartbeats
- [ ] Add background task that stores own heartbeats in the node database #11
- [ ] Add background task that checks other machine's heartbeats and removes machines from a node if they did not respond to long #11

## One to One Communication

### Naming Service based Management
- [ ] Add keygroup configuration control methods to sender
- [ ] Add response processing to all methods
- [ ] Add node management methods (e.g. to update a node configuration when a machine is added/removed)
- [ ] Add a background task that periodically polls the naming service about the newest configurations

### Handling Missed Messages
- [ ] Messaging data needs to be stored after each data related publish
- [ ] Add sender/receiver capabilities
- [ ] Write logic that uses functionality

## Controlling FBase with Clients
 - [ ] Enable asymmetric encryption
 - [ ] Identify and add missing control methods