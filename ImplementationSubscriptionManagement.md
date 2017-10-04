# Implementation Subscription Management

Numbers are not as written in thesis

## Events

### [1] Missing heartbeats detected

- [x] Background task that stores own heartbeats in the node database
- [x] Background task to detect missing heartbeats
- [x] Detected: run RemoveMachineFromNodeTask

### [2] No responsibility detected
- [x] Background task to detect missing responsibilities
- [x] Detected: run UpdateKeygroupSubscriptionsTask

### [3] Lost responsibility detected

- [x] Background task to detect lost responsibilities for each keygroup for which subscriptions exist
- [x] Detected: run UpdateKeygroupSubscriptionsTask

### [4] Client updates/deletes keygroup via node

- [x] Add client keygroup update/delete functionality incl. naming service validation
- [x] Naming service approves: run UpdateKeygroupConfigTask

### [5] Not interpretable message

- [x] Add encryption exception handling to subscriber
- [x] Catched: get newest keygroup config version from naming service
- [x] Run UpdateKeygroupConfigTask

### [6] Periodic configuration update

- [x] Background task that periodically polls all responsible keygroup configurations and node configurations for nodes present in the keygroups
- [x] Keygroup: run UpdateKeygroupConfigTask
- [x] Node: UpdateForeignNodeConfigTask

### [7] Recognize foreign keygroup update
Needed if [4] is performed by other node

- [x] Add background task that checks whether version of keygroup config changed
- [x] Changed: Run UpdateKeygroupSubscriptionsTask

### [8] Subscriber receives configuration update

- [x] Add configuration update handling to subscriber
- [x] Run UpdateKeygroupConfigTask or UpdateForeignNodeConfigTask

## Tasks (not background)

### [D] UpdateKeygroupSubscriptionsTask

1. Remove all subscriptions for a given keygroup
2. Create new subscriptions if all `true`:
  - node still apart of the keygroup
  - keygroup not tombstoned
  - no machine responsible yet/I am responsible
3. Update responsibility table

### [B] UpdateKeygroupConfigTask
Only run if version differs

1. Put configuration in node database
2. Run UpdateKeygroupSubscriptionsTask
3. Publish config to all subscribers if started by [4]

### [C] UpdateForeignNodeConfigTask
Only run if the machines changed

1. Put configuration in node database
2. Get all keygroups in which node participates
  * Run UpdateKeygroupSubscriptionsTask for keygroup

### [A] RemoveMachineFromNodeTask

1. Remove machine from responsibility table
2. Remove machine from heartbeats table
3. Rebuild nodeconfig and send to naming service
4. Rebuild nodeconfig and publish to all subscribers for all keygroups
5. UpdateKeygroupSubscriptionsTask

### AddMachineToNodeTask
Not used here, is used by startup functionality

1. Put heartbeat into heartbeats
2. Rebuild nodeconfig and send to naming service
3. Rebuild nodeconfig and publish to all subscribers for all keygroups (must be done by another node!)
