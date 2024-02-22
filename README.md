# az-eventhubs-cli

browse through the content of an eventhubs topic ...

- https://learn.microsoft.com/en-us/azure/event-hubs/apache-kafka-frequently-asked-questions
- https://learn.microsoft.com/en-us/java/api/com.azure.messaging.eventhubs.models.eventhubconnectionstringproperties?view=azure-java-stable
  

Note: it uses the "$Default" consumer-group, where offsets are not being saved

```

$ make build

$ ./ehctl --help
$ ./ehctl peek --help
$ ./ehctl peek --connection-string="Endpoint=sb://<domain>.servicebus.windows.net/;SharedAccessKeyName=PreviewDataPolicy;SharedAccessKey=<accessKey>;EntityPath=<topic>"

```