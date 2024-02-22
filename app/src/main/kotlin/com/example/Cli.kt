package com.example

import com.example.cmd.peek.PeekCommand
import com.github.ajalt.clikt.core.NoOpCliktCommand
import com.github.ajalt.clikt.core.subcommands

class Cli : NoOpCliktCommand(name = "ehctl", printHelpOnEmptyArgs = true) {
    override fun run() {}
}

fun main(args: Array<String>) {
    Cli()
        .subcommands(
            PeekCommand(),
        )
        .main(args)
}