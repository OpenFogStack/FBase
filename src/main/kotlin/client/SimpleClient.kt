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
    fun addToKeygroup(keygroupId: String, clientIds: List<String>? = null, replicaNodeIds: List<String>? = null,
                      ttl: Int = -1, triggerNodeIds: List<String>? = null) {

        val keygroupID: KeygroupID? = KeygroupID.createFromString(keygroupId)
        if (keygroupID == null) {
            logger.info("Cannot update keygroup with the id $keygroupId as the id is not valid")
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
        if (clientIds != null) {
            for (c in clientIds.map { ClientID(it) }) {

                if (c in keygroupConfig.clients) {
                    logger.info("Client $c has already access to the keygroup")
                    continue
                }

                logger.info("Added client to keygroup: ${request.addClient(keygroupID, c)}")
            }
        }

        // add replica nodes
        if (replicaNodeIds != null) {
            for (r in replicaNodeIds.map { NodeID(it) }) {

                if (keygroupConfig.containsReplicaNode(r)) {
                    logger.info("Replica node is already part of the keygroup")
                    continue
                }

                val replicaNodeConfig = ReplicaNodeConfig(r, ttl)
                logger.info("Added replica node to keygroup: ${request.addReplicaNode(keygroupID, replicaNodeConfig)}")
            }
        }

        // add trigger nodes
        if (triggerNodeIds != null) {
            for (t in triggerNodeIds.map { NodeID(it) }) {

                if (keygroupConfig.containsTriggerNode(t)) {
                    logger.info("Trigger node is already part of the keygroup")
                    continue
                }

                val triggerNodeConfig = TriggerNodeConfig(t)
                logger.info("Added trigger node to keygroup: ${request.addTriggerNode(keygroupID, triggerNodeConfig)}")
            }
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
}
