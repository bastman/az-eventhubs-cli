package com.example.cmd.peek

import com.azure.messaging.eventhubs.models.PartitionEvent

class EhPartitionEventHandler(private val cmd: PeekCommand) {

    fun handleEvent(evt: PartitionEvent) {
        cmd.echo("event at: ${evt.data.sequenceNumber}/${evt.data.enqueuedTime} => ${evt.data.bodyAsString}")
        cmd.echo("")
    }
}