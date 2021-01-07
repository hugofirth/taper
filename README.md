## Readme for TAPER, part of org.gdget.experimental

#### Anatomy of the project

Key datastructures/algos: 

* **TPSTry** located in: `src/main/scala/org/gdget/experimental/graph/TraversalPatternSummary.scala`
* **Vertex swapping** located in: `src/main/scala/org/gdget/experimental/graph/partition/Partition.scala`, particularly `Refinable.refine`

#### Building the project

The project may be built using the following command:

`mvn clean package`

Experiments may be run by executing the produced jar:

`java -jar contextual-stability-0.1-SNAPSHOT.jar`

Change which experiment will be executed by editing `src/main/scala/org/gdget/experimental/Main.scala` and recompiling.
