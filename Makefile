
# ## gradle
GRADLE_EXE=./gradlew


PWD=$(shell pwd)

print-%: ; @echo $*=$($*)
guard-%:
	@test ${${*}} || (echo "FAILED! Environment variable $* not set " && exit 1)
	@echo "-> use env var $* = ${${*}}";

.PHONY : help
help : Makefile
	@sed -n 's/^##//p' $<




## clean   : clean
clean:
	$(GRADLE_EXE) clean

## build   : clean,build,fatjar
build: clean
	$(GRADLE_EXE) shadowJar
	java -jar app/build/libs/app-all.jar --help
	java -jar app/build/libs/app-all.jar peek --help