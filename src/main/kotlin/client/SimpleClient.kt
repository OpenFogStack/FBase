package client

import crypto.CryptoProvider
import model.JSONable
import model.config.KeygroupConfig
import model.config.NodeConfig
import model.config.ReplicaNodeConfig
import model.config.TriggerNodeConfig
import model.data.ClientID
import model.data.KeygroupID
import model.data.NodeID
import org.apache.log4j.Logger
import java.io.File


private val logger = Logger.getLogger(Client::class.java.name)

/**
 * A convenience client interface
 *
 * @author jonathanhasenburg
 */
class Client(val address: String, val port: Int) {

    /**
     * Adds the given information to the Keygroup with [keygroupId]; creates the keygroup if it did not exist yet.
     * If information is already present in the keygroup, this piece of information is ignored.
     *
     * Note, that all clients and nodes have to be registered at the naming service before they can be added.
     *
     * @param keygroupId - the id of the keygroup
     * @param clientIds - list of new client ids
     * @param replicaNodeIds - list of new replica node ids
     * @param ttl - the time to live value in seconds that is used for newly added replica nodes (default = infinite)
     * @param triggerNodeIds - list of new trigger node ids
     */
    fun addToKeygroup(keygroupId: String, clientIds: List<String> = emptyList(),
                      replicaNodeIds: List<String> = emptyList(), ttl: Int = -1,
                      triggerNodeIds: List<String> = emptyList()) {

        // check whether valid keygroupID
        val keygroupID = KeygroupID.createFromString(keygroupId)?: run {
            logger.warn("Cannot update keygroup with the id $keygroupId as the id is not valid")
            return
        }

        val request = KeygroupRequest(address, port)

        // get current config first
        var keygroupConfig: KeygroupConfig? = request.updateLocalKeygroupConfig(keygroupID)

        // if not existent, create keygroup
        if (keygroupConfig == null) {
            keygroupConfig = KeygroupConfig(keygroupID, "passw", CryptoProvider.EncryptionAlgorithm.AES)
            val kr = KeygroupRequest(address, port)
            logger.info("Created keygroup: ${kr.createKeygroup(keygroupConfig)}")
        }

        // add clients
        for (c in clientIds.map { ClientID(it) }) {

            if (c in keygroupConfig.clients) {
                logger.info("Client $c has already access to the keygroup")
                continue
            }

            logger.info("Added client $c to keygroup: ${request.addClient(keygroupID, c)}")
        }

        // add replica nodes
        for (r in replicaNodeIds.map { NodeID(it) }) {

            if (keygroupConfig.containsReplicaNode(r)) {
                logger.info("Replica node $r is already part of the keygroup")
                continue
            }

            val replicaNodeConfig = ReplicaNodeConfig(r, ttl)
            logger.info("Added replica node $r to keygroup: ${request.addReplicaNode(keygroupID, replicaNodeConfig)}")
        }

        // add trigger nodes
        for (t in triggerNodeIds.map { NodeID(it) }) {

            if (keygroupConfig.containsTriggerNode(t)) {
                logger.info("Trigger node $t is already part of the keygroup")
                continue
            }

            val triggerNodeConfig = TriggerNodeConfig(t)
            logger.info("Added trigger node $t to keygroup: ${request.addTriggerNode(keygroupID, triggerNodeConfig)}")
        }
    }

    fun removeFromKeygroup(keygroupId: String, clientIds: List<String> = emptyList(),
                           replicaNodeIds: List<String> = emptyList(), triggerNodeIds: List<String> = emptyList()) {

        // check whether valid keygroupID
        val keygroupID = KeygroupID.createFromString(keygroupId)?: run {
            logger.warn("Cannot remove items from keygroup with the id $keygroupId as the id is not valid")
            return
        }

        val request = KeygroupRequest(address, port)

        // get current config first, return if not null
        val keygroupConfig = request.updateLocalKeygroupConfig(keygroupID)?: run {
            logger.warn("Keygroup $keygroupId does not exist")
            return
        }

        // remove clients
        for (c in clientIds.map { ClientID(it) }) {

            if (c !in keygroupConfig.clients) {
                logger.info("Keygroup has no client $c")
                continue
            }

            logger.info("Removed client $c from keygroup: ${request.deleteClient(keygroupID, c)}")
        }

        // remove replica nodes
        for (r in replicaNodeIds.map { NodeID(it) }) {

            if (!keygroupConfig.containsReplicaNode(r)) {
                logger.info("Keygroup has no replica node $r")
                continue
            }

            logger.info("Removed replica node $r from keygroup: ${request.deleteNode(keygroupID, r)}")
        }

        // remove trigger nodes
        for (t in triggerNodeIds.map { NodeID(it) }) {

            if (keygroupConfig.containsTriggerNode(t)) {
                logger.info("Keygroup has no trigger node $t")
                continue
            }

            logger.info("Removed trigger node $t from keygroup: ${request.deleteNode(keygroupID, t)}")
        }
    }

}

fun main(args: Array<String>) {
    val address = "localhost"
    val port = 8081
    val keygroupId = "smarthome/lights/floor1"

    val c = Client(address, port)

    // add fake node N2
    val nr = NodeRequest(address, port)
    val nodeConfig = JSONable.fromJSON(File("src/main/resources/n1.json").inputStream(), NodeConfig::class.java)
    nodeConfig.nodeID = NodeID("N2")
    nr.createNodeConfig(nodeConfig)

    c.addToKeygroup(keygroupId, replicaNodeIds = listOf("N2"), ttl = 120)

    // remove N3 (must fail)
    c.removeFromKeygroup(keygroupId, replicaNodeIds = listOf("N3"))

    // remove ourselves N1
    c.removeFromKeygroup(keygroupId, replicaNodeIds = listOf("N1"))

}
