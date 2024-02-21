# az-eventhubs-cli

browse through the content of an eventhub topic ...

-  https://learn.microsoft.com/en-us/java/api/com.azure.messaging.eventhubs.models.eventhubconnectionstringproperties?view=azure-java-stable
   
```

$ make build

$ java -jar app/build/libs/app-all.jar --help
$ java -jar app/build/libs/app-all.jar peek --help
$ java -jar app/build/libs/app-all.jar peek --connection-string="Endpoint=sb://<domain>.servicebus.windows.net/;SharedAccessKeyName=PreviewDataPolicy;SharedAccessKey=<accessKey>;EntityPath=<topic>"

```