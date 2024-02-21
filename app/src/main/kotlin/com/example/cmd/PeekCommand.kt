package com.example.cmd

import com.azure.core.credential.AzureNamedKeyCredential
import com.azure.messaging.eventhubs.EventHubClientBuilder
import com.azure.messaging.eventhubs.EventHubConsumerClient
import com.azure.messaging.eventhubs.models.EventHubConnectionStringProperties
import com.azure.messaging.eventhubs.models.EventPosition
import com.azure.messaging.eventhubs.models.PartitionEvent
import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.core.context
import com.github.ajalt.clikt.core.terminal
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.required
import com.github.ajalt.clikt.parameters.options.validate
import com.github.ajalt.clikt.parameters.types.boolean
import com.github.ajalt.clikt.parameters.types.int
import com.github.ajalt.clikt.parameters.types.long
import com.github.ajalt.clikt.parameters.types.restrictTo
import com.github.ajalt.mordant.terminal.YesNoPrompt
import mu.KLogging
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit

class PeekCommand : CliktCommand(name = "peek") {
    companion object : KLogging()

    init {
        context {
            readEnvvarBeforeValueSource = false
        }
    }

    val optionConnectionString: String by option(
        help = "az eh connectionString incl. entityPath. e.g.: Endpoint=sb://<domain>.servicebus.windows.net/;SharedAccessKeyName=PreviewDataPolicy;SharedAccessKey=<accessKey>;EntityPath=<topic>",
        envvar = "AZ_EVENTHUBS_CLI_CONNECTION_STRING",
        names = arrayOf("--connection-string"),
    ).required()
        .validate {
            require(it.isNotBlank()) { "--connection-string must not be blank" }
            val properties = EventHubConnectionStringProperties.parse(optionConnectionString)
            val entityPath: String = (properties.entityPath ?: "")
            if (entityPath.isBlank()) {
                fail("--connection-string must contain 'entityPath'")
            }
        }


    val optionConsumerGroupName: String by option(
        names = arrayOf("--consumer-group"),
        help = "e.g. ${EventHubClientBuilder.DEFAULT_CONSUMER_GROUP_NAME} - this consumer-group does not store offsets"
    ).default(EventHubClientBuilder.DEFAULT_CONSUMER_GROUP_NAME)
        .validate { require(it.isNotBlank()) { "--consumer-group must not be blank" } }

    val optionPartitionId: String by option(
        names = arrayOf("--partitionId")
    ).default("0")
        .validate { require(it.isNotBlank()) { "--partitionId must not be blank" } }


    val optionSeekStartTimeRaw: String? by option(
        names = arrayOf("--seek-start-time"),
        help = "e.g.: ${(Instant.now() - Duration.ofDays(8)).truncatedTo(ChronoUnit.SECONDS)}"
    ).validate {
        try {
            Instant.parse(it)
        } catch (e: Exception) {
            fail("--seek-start-time must be of type Instant")
        }
    }

    val optionSeekStartTime: Instant? by lazy {
        when (optionSeekStartTimeRaw) {
            null -> null
            else -> Instant.parse(optionSeekStartTimeRaw)
        }
    }

    val optionSeekStartSequenceNumber: Long? by option(
        names = arrayOf("--seek-start-sequence-number"),
        help = "aka kafka-api: 'start-offset'"
    ).long()
        .restrictTo(0..Long.MAX_VALUE)

    val optionPollMaxWaitTimeInSeconds: Int by option(
        names = arrayOf("--poll-max-wait-time-in-seconds"),
    ).int()
        .restrictTo(1..60)
        .default(10)
    val optionPollMaxWaitTime: Duration by lazy { Duration.ofSeconds(optionPollMaxWaitTimeInSeconds.toLong()) }

    val optionPollMaxMessages: Int by option(
        names = arrayOf("--poll-max-messages"),
    ).int()
        .restrictTo(1..100)
        .default(2)

    val optionPollStopOnNoEventsReceived: Boolean by option(
        names = arrayOf("--poll-stop-on-no-events-received"),
    )
        .boolean()
        .default(true)

    val optionPollStopOnSeekEndTimeRaw: String? by option(
        names = arrayOf("--poll-stop-on-seek-end-time"),
        help = "e.g.: ${(Instant.now()).truncatedTo(ChronoUnit.SECONDS)}"
    ).validate {
        try {
            Instant.parse(it)
        } catch (e: Exception) {
            fail("--poll-stop-on-seek-end-time must be of type Instant")
        }
    }

    val optionPollStopOnSeekEndTime: Instant? by lazy {
        when (optionPollStopOnSeekEndTimeRaw) {
            null -> null
            else -> Instant.parse(optionPollStopOnSeekEndTimeRaw)
        }
    }

    val optionPollStopOnSeekEndSequenceNumber: Long? by option(
        names = arrayOf("--poll-stop-on-seek-end-sequence-number"),
        help = "aka kafka-api: 'end-offset'"
    ).long()
        .restrictTo(0..Long.MAX_VALUE)

    val optionPollStopOnUserConfirmationPrompt: Boolean by option(
        names = arrayOf("--poll-stop-on-user-confirmation-prompt"),
    )
        .boolean()
        .default(true)

    private val ehProperties: EventHubConnectionStringProperties by lazy {
        EventHubConnectionStringProperties.parse(optionConnectionString)
    }
    private val ehCredential: AzureNamedKeyCredential by lazy {
        AzureNamedKeyCredential(
            ehProperties.sharedAccessKeyName,
            ehProperties.sharedAccessKey
        )
    }
    private val ehConsumer: EventHubConsumerClient by lazy {
        val ehEntityPath: String = (ehProperties.entityPath ?: "")
        EventHubClientBuilder()
            .credential(ehProperties.fullyQualifiedNamespace, ehEntityPath, ehCredential)
            .consumerGroup(optionConsumerGroupName)
            .buildConsumerClient()
    }

    private fun ehSeekStartPositionFromCliOptions(): EventPosition {
        val seekStartSequenceNumberValue: Long? = optionSeekStartSequenceNumber
        if (seekStartSequenceNumberValue != null) {
            return EventPosition.fromSequenceNumber(seekStartSequenceNumberValue, true)
        }
        val seekStartTimeValue: Instant? = optionSeekStartTime
        if (seekStartTimeValue != null) {
            return EventPosition.fromEnqueuedTime(seekStartTimeValue)
        }
        return EventPosition.fromSequenceNumber(-1)
    }

    override fun run() {
        echo("==== ${this::class.java.name} START ... ====")

        val startingPosition: EventPosition = ehSeekStartPositionFromCliOptions()
        var fromPosition: EventPosition = startingPosition
        var doLoop = true
        while (doLoop) {
            echo("poll maxMessages: $optionPollMaxMessages from partition: $optionPartitionId fromPosition: $fromPosition timeout: $optionPollMaxWaitTime ...")
            val pollOutcome: EhPollOutcome = ehPoll(
                pollFromPartitionId = this.optionPartitionId,
                pollMaxMessages = this.optionPollMaxMessages,
                pollFromPosition = fromPosition,
                pollMaxWaitTime = this.optionPollMaxWaitTime,
            )
            if (pollOutcome.lastEvent != null) {
                val nextPosition: EventPosition =
                    EventPosition.fromSequenceNumber(pollOutcome.lastEvent.data.sequenceNumber, false)
                fromPosition = nextPosition
            }

            pollOutcome.events.forEach(::onEventReceived)

            if (pollOutcome.events.isEmpty() && optionPollStopOnNoEventsReceived) {
                echo("stop polling. reason: no events received. option --poll-stop-on-no-events-received: $optionPollStopOnNoEventsReceived")
                break
            }

            if (shouldStopPollingByOptionSeekEndSequenceNumber(
                    optionValue = optionPollStopOnSeekEndSequenceNumber,
                    pollOutcome
                )
            ) {
                echo("stop polling. reason: option --poll-stop-on-seek-end-sequence-number: $optionPollStopOnSeekEndSequenceNumber")
                break
            }
            if (shouldStopPollingByOptionSeekEndTime(optionValue = optionPollStopOnSeekEndTime, pollOutcome)) {
                echo("stop polling. reason: option --poll-stop-on-seek-end-time: $optionPollStopOnSeekEndTime")
                break
            }

            if (optionPollStopOnUserConfirmationPrompt) {
                if (YesNoPrompt("Continue?", terminal).ask() == false) {
                    echo("stop polling. reason: abort by user. option --poll-stop-on-user-confirmation-prompt $optionPollStopOnUserConfirmationPrompt")
                    break
                }
            }


        }

        echo("=== ${this::class.java.name} DONE . ===")
        System.exit(0)
    }

    private fun onEventReceived(evt: PartitionEvent) {
        run {
            val optionValue: Instant? = optionPollStopOnSeekEndTime
            val evtValue: Instant = evt.data.enqueuedTime
            if (optionValue != null && evtValue > optionValue) {
                return
            }
        }
        run {
            val optionValue: Long? = optionPollStopOnSeekEndSequenceNumber
            val evtValue: Long = evt.data.sequenceNumber
            if (optionValue != null && evtValue > optionValue) {
                return
            }
        }

        echo("${evt.data.sequenceNumber}/${evt.data.enqueuedTime} => ${evt.data.bodyAsString}")
    }

    private fun ehPoll(
        pollFromPartitionId: String,
        pollMaxMessages: Int,
        pollMaxWaitTime: Duration,
        pollFromPosition: EventPosition
    ): EhPollOutcome {
        val events: List<PartitionEvent> = ehConsumer.receiveFromPartition(
            pollFromPartitionId,
            pollMaxMessages,
            pollFromPosition,
            pollMaxWaitTime
        ).toList()
        val pollOutcome = EhPollOutcome(
            partitionId = pollFromPartitionId,
            pollMaxMessages = pollMaxMessages,
            pollFromPosition = pollFromPosition,
            pollMaxWaitTime = pollMaxWaitTime,
            events = events,
        )
        return pollOutcome
    }

    private fun shouldStopPollingByOptionSeekEndSequenceNumber(
        optionValue: Long?,
        ehPollOutcome: EhPollOutcome
    ): Boolean {
        if (optionValue == null) {
            return false
        }
        val pollOutcomeValue: Long = ehPollOutcome.lastEventSequenceNumber ?: return false
        return pollOutcomeValue >= optionValue
    }

    private fun shouldStopPollingByOptionSeekEndTime(optionValue: Instant?, ehPollOutcome: EhPollOutcome): Boolean {
        if (optionValue == null) {
            return false
        }
        val pollOutcomeValue: Instant = ehPollOutcome.lastEventEnqueuedTime ?: return false
        return pollOutcomeValue >= optionValue
    }


}

data class EhPollOutcome(
    // input
    val partitionId: String,
    val pollFromPosition: EventPosition,
    val pollMaxMessages: Int,
    val pollMaxWaitTime: Duration,
    // output
    val events: List<PartitionEvent>
) {
    val lastEvent: PartitionEvent? = events.lastOrNull()
    val lastEventSequenceNumber: Long? = lastEvent?.data?.sequenceNumber
    val lastEventEnqueuedTime: Instant? = lastEvent?.data?.enqueuedTime
}


