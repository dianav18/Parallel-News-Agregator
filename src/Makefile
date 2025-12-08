JAR_NAME = Tema1.jar
SOURCE_JAR = target/tema1-1.0.jar
MAVEN_FLAGS = -q

.PHONY: build run clean

build:
	mvn package $(MAVEN_FLAGS)
	cp $(SOURCE_JAR) $(JAR_NAME)

run:
	java -jar $(JAR_NAME) $(ARGS)

clean:
	mvn clean $(MAVEN_FLAGS)
	rm -f $(JAR_NAME)