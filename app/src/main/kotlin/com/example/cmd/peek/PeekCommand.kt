package com.example.cmd.peek

import com.azure.core.credential.AzureNamedKeyCredential
import com.azure.messaging.eventhubs.EventHubClientBuilder
import com.azure.messaging.eventhubs.EventHubConsumerClient
import com.azure.messaging.eventhubs.models.EventHubConnectionStringProperties
import com.azure.messaging.eventhubs.models.EventPosition
import com.azure.messaging.eventhubs.models.PartitionEvent
import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.core.context
import com.github.ajalt.clikt.core.terminal
import com.github.ajalt.clikt.output.MordantHelpFormatter
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

class PeekCommand() : CliktCommand(name = "peek", printHelpOnEmptyArgs = false) {
    companion object : KLogging()


    enum class CommandOption(val optionName: String) {
        ConnectionString("--connection-string"),
        ConsumerGroupName("--consumer-group"),
        PartitionId("--partitionId"),
        SeekStartTime("--seek-start-time"),
        SeekStartSequenceNumber("--seek-start-sequence-number"),
        PollMaxWaitTimeInSeconds("--poll-max-wait-time-in-seconds"),
        PollMaxMessages("--poll-max-messages"),
        PollStopOnNoEventsReceived("--poll-stop-on-no-events-received"),
        PollStopOnSeekEndTime("--poll-stop-on-seek-end-time"),
        PollStopOnSeekEndSequenceNumber("--poll-stop-on-seek-end-sequence-number"),
        PollStopOnUserConfirmationPrompt("--poll-stop-on-user-confirmation-prompt")
        ;
    }


    init {
        context {
            readEnvvarBeforeValueSource = false
            helpFormatter = { MordantHelpFormatter(it, showDefaultValues = true) }
        }
    }

    private val ehPartitionEventHandler: EhPartitionEventHandler by lazy { EhPartitionEventHandler(cmd = this) }

    val optionConnectionString: String by option(
        help = "az eh connectionString incl. entityPath. e.g.: Endpoint=sb://<domain>.servicebus.windows.net/;SharedAccessKeyName=PreviewDataPolicy;SharedAccessKey=<accessKey>;EntityPath=<topic>",
        envvar = "AZ_EVENTHUBS_CLI_CONNECTION_STRING",
        names = arrayOf(CommandOption.ConnectionString.optionName),
    ).required()
        .validate {
            require(it.isNotBlank()) { "--connection-string must not be blank" }
            val properties: EventHubConnectionStringProperties =
                EventHubConnectionStringProperties.parse(optionConnectionString)
            val entityPath: String = (properties.entityPath ?: "")
            if (entityPath.isBlank()) {
                fail("${CommandOption.ConnectionString.optionName} must contain 'entityPath'")
            }
        }


    val optionConsumerGroupName: String by option(
        names = arrayOf(CommandOption.ConsumerGroupName.optionName),
        help = "e.g. ${EventHubClientBuilder.DEFAULT_CONSUMER_GROUP_NAME} - this special consumer-group does not store any offsets"
    ).default(EventHubClientBuilder.DEFAULT_CONSUMER_GROUP_NAME)
        .validate { require(it.isNotBlank()) { "${CommandOption.ConsumerGroupName.optionName} must not be blank" } }

    val optionPartitionId: String by option(
        names = arrayOf(CommandOption.PartitionId.optionName)
    ).default("0")
        .validate { require(it.isNotBlank()) { "${CommandOption.PartitionId.optionName} must not be blank" } }


    val optionSeekStartTimeRaw: String? by option(
        names = arrayOf(CommandOption.SeekStartTime.optionName),
        help = "e.g.: ${(Instant.now() - Duration.ofDays(8)).truncatedTo(ChronoUnit.SECONDS)}"
    ).validate {
        try {
            Instant.parse(it)
        } catch (e: Exception) {
            fail("${CommandOption.SeekStartTime.optionName} must be of type Instant")
        }
    }

    val optionSeekStartTime: Instant? by lazy {
        when (optionSeekStartTimeRaw) {
            null -> null
            else -> Instant.parse(optionSeekStartTimeRaw)
        }
    }

    val optionSeekStartSequenceNumber: Long? by option(
        names = arrayOf(CommandOption.SeekStartSequenceNumber.optionName),
        help = "aka kafka-api: 'start-offset'"
    ).long()
        .restrictTo(0..Long.MAX_VALUE)

    val optionPollMaxWaitTimeInSeconds: Int by option(
        names = arrayOf(CommandOption.PollMaxWaitTimeInSeconds.optionName),
    ).int()
        .restrictTo(1..60)
        .default(10)
    val optionPollMaxWaitTime: Duration by lazy { Duration.ofSeconds(optionPollMaxWaitTimeInSeconds.toLong()) }

    val optionPollMaxMessages: Int by option(
        names = arrayOf(CommandOption.PollMaxMessages.optionName),
    ).int()
        .restrictTo(1..100)
        .default(2)

    val optionPollStopOnNoEventsReceived: Boolean by option(
        names = arrayOf(CommandOption.PollStopOnNoEventsReceived.optionName),
    )
        .boolean()
        .default(true)

    val optionPollStopOnSeekEndTimeRaw: String? by option(
        names = arrayOf(CommandOption.PollStopOnSeekEndTime.optionName),
        help = "e.g.: ${(Instant.now()).truncatedTo(ChronoUnit.SECONDS)}"
    ).validate {
        try {
            Instant.parse(it)
        } catch (e: Exception) {
            fail("${CommandOption.PollStopOnSeekEndTime.optionName} must be of type Instant")
        }
    }

    val optionPollStopOnSeekEndTime: Instant? by lazy {
        when (optionPollStopOnSeekEndTimeRaw) {
            null -> null
            else -> Instant.parse(optionPollStopOnSeekEndTimeRaw)
        }
    }

    val optionPollStopOnSeekEndSequenceNumber: Long? by option(
        names = arrayOf(CommandOption.PollStopOnSeekEndSequenceNumber.optionName),
        help = "aka kafka-api: 'end-offset'"
    ).long()
        .restrictTo(0..Long.MAX_VALUE)

    val optionPollStopOnUserConfirmationPrompt: Boolean by option(
        names = arrayOf(CommandOption.PollStopOnUserConfirmationPrompt.optionName),
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
        echo("")

        val qName: String =
            listOf(ehProperties.fullyQualifiedNamespace, ehProperties.entityPath).joinToString(
                separator = ":"
            )
        val partitionIds: List<String> = ehConsumer.partitionIds.toList()
        echo("eventhub: $qName has the following partitionIds: $partitionIds")
        echo("")
        val startingPosition: EventPosition = ehSeekStartPositionFromCliOptions()
        var fromPosition: EventPosition = startingPosition
        var doLoop = true
        while (doLoop) {
            val qName: String =
                listOf(ehProperties.fullyQualifiedNamespace, ehProperties.entityPath, optionPartitionId).joinToString(
                    separator = ":"
                )
            echo("eventhub: $optionConsumerGroupName@$qName")
            echo("=> poll maxMessages: $optionPollMaxMessages from partition: $optionPartitionId fromPosition: $fromPosition timeout: $optionPollMaxWaitTime ...")
            echo("")

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
                echo("")
                echo("stop polling. reason: no events received. option ${CommandOption.PollStopOnNoEventsReceived.optionName}: $optionPollStopOnNoEventsReceived")
                break
            }

            if (shouldStopPollingByOptionSeekEndSequenceNumber(
                    optionValue = optionPollStopOnSeekEndSequenceNumber,
                    pollOutcome
                )
            ) {
                echo("")
                echo("stop polling. reason: option ${CommandOption.PollStopOnSeekEndSequenceNumber.optionName}: $optionPollStopOnSeekEndSequenceNumber")
                break
            }
            if (shouldStopPollingByOptionSeekEndTime(optionValue = optionPollStopOnSeekEndTime, pollOutcome)) {
                echo("")
                echo("stop polling. reason: option ${CommandOption.PollStopOnSeekEndTime.optionName}: $optionPollStopOnSeekEndTime")
                break
            }

            if (optionPollStopOnUserConfirmationPrompt) {
                if (YesNoPrompt("Continue?", terminal).ask() == false) {
                    echo("")
                    echo("stop polling. reason: abort by user. option ${CommandOption.PollStopOnUserConfirmationPrompt}: $optionPollStopOnUserConfirmationPrompt")
                    break
                }
            }


        }

        echo("")
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

        ehPartitionEventHandler.handleEvent(evt = evt)
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

}




