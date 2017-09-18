# Implementation Subscription Management

## Events

### [1] Missing heartbeats detected

- [ ] Background task that stores own heartbeats in the node database
- [ ] Background task to detect missing heartbeats
- [ ] Detected: run RemoveMachineFromNodeTask

### [2] No responsibility detected

- [ ] Background task to detect missing responsibilities
- [ ] Detected: run UpdateKeygroupSubscriptionsTask

### [3] Lost responsibility detected

- [ ] Background task to detect lost responsibilities for each keygroup for which subscriptions exist
- [ ] Detected: run UpdateKeygroupSubscriptionsTask

### [4] Client updates/deletes keygroup via node

- [ ] Add client keygroup update/delete functionality incl. naming service validation
- [ ] Naming service approves: run UpdateKeygroupConfigTask

### [5] Not interpretable message

- [ ] Add encryption exception handling to subscriber
- [ ] Catched: get newest keygroup config version from naming service and check on changes
- [ ] Change detected: run UpdateKeygroupConfigTask

### [6] Periodic configuration update

- [ ] Background task that periodically polls all configurations
- [ ] Change detected: run UpdateKeygroupConfigTask or UpdateForeignNodeConfigTask

### [7] Subscriber receives configuration update

- [ ] Add configuration update handling to subscriber
- [ ] Run UpdateKeygroupConfigTask or UpdateForeignNodeConfigTask

### [8] Recognize foreign configuration update
Needed if [4] or [6] are performed by other node

- [ ] Add background task that checks whether version of keygroup config changed
- [ ] Changed: Run UpdateKeygroupSubscriptionsTask
- [ ] Add background task that checks whether internal version vectors of node config changed
- [ ] Changed: Run UpdateKeygroupSubscriptionsForChangedNodeConfigTask

## Tasks (not background)

### [D] UpdateKeygroupSubscriptionsTask

1. Remove all subscriptions for a given keygroup
2. Create new subscriptions if all `true`:
  - node still apart of the keygroup
  - keygroup not tombstoned
  - no machine responsible yet/I am responsible
3. Update responsibility table

### [B] UpdateKeygroupConfigTask

1. Store configuration in node database and increment internal version vector if version differs from stored version
2. Run UpdateKeygroupSubscriptionsTask
3. Publish config to all subscribers if started by [4]

### [C] UpdateForeignNodeConfigTask

1. Store configuration in node database and increment internal version vector if version differs from stored version (if the received configuration had a version vector, it is ignored, because it is the internal vector of another node)
2. Get all keygroups in which node participates
3. Run UpdateKeygroupSubscriptionsForChangedNodeConfigTask

### [A] RemoveMachineFromNodeTask

1. Remove machine from node config
  * Store config in node database, increment internal version vector
  * Notify naming service about updated configuration
  * Publish node config to all subscribers for all keygroups
3. Remove machine from responsibility table
4. Remove machine from heartbeats table
5. UpdateKeygroupSubscriptionsTask

### AddMachineToNodeTask
Not used here, is used by startup functionality

1. Add machine to node config
  * Store config in node database, increment internal version vector
  * Notify naming service about updated configuration
  * Publish node config to all subscribers for all keygroups
2. Run UpdateOwnNodeConfigTask
