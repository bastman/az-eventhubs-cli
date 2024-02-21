package com.example

import com.example.cmd.PeekCommand
import com.github.ajalt.clikt.core.NoOpCliktCommand
import com.github.ajalt.clikt.core.subcommands

class Cli : NoOpCliktCommand(name = "eh", printHelpOnEmptyArgs = true) {
    override fun run() {}
}

fun main(args: Array<String>) {
    Cli()
        .subcommands(
            PeekCommand(),
        )
        .main(args)
}