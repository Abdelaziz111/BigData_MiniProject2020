# Compte rendu Bid Data Mini_ProjectII
réalisé par le binome : Abdelaziz CHERIFI & Joseph AYEBOU

### Question 1
#### Configuration et importation 

* Dans ce projet, nous avons décidés de travailler avec le langage Scala; pour cela, on doit effectuer certaine configuration pour que nous puissons faire ce travail.
* Pour cela, nous avons téléchager /spark-3.0.0-preview2-bin-hadoop2.7 et on l'a rajouté au variable d'environnement Windwos, ensuite rajouté graphFrame au SPARK_HOME en ligne de commande pour pouvoir importer l'API GraphGrame et faire le TP.
* Ensuite, on a procédé à la déclaration de SrarkSession comme ceci :


```scala
    System.setProperty("hadoop.home.dir", "C:/Program Files/spark-3.0.0-preview2-bin-hadoop2.7")

      val spark = SparkSession.builder.appName("BigDataProjet").config("spark.master", "local[*]").getOrCreate
          spark.sparkContext.setLogLevel("ERROR")

```    

### Question 2
#### Create Vertices (airports) and Edges
1. Tout à bord, on charge les deux fichiers qui vont nous servir de *Vertices* et de *Edges* (departuredelays.csv et airports.dat.txt) comme ceci :  *wordsFromFile*  en utilisant al fonction ftalMap
```scala
     val tripdelaysFilePath = "src/main/scala/exemple/data/departuredelays.csv"
          val airportsnaFilePath = "src/main/scala/exemple/data/airports.dat.txt"

          val airportsna = spark.sqlContext.read.format("com.databricks.spark.csv").
            option("header", "true").
            option("inferschema", "true").
            option("delimiter", "\t").
            load(airportsnaFilePath)

```
2. Ensuite on passe à la création des *Vertices* et de *Edges* et pour cela nous avons besoins de créer deux **Dataset** qui vont contenir le contenu des deux fichiers comme ceci : 
```scala
    
    val departureDelays = spark.sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load(tripdelaysFilePath)
          departureDelays.createOrReplaceGlobalTempView("departureDelays")
          departureDelays.cache()

    val airports = spark.sqlContext.sql("select f.IATA, f.City, f.State, f.Country from global_temp.airports_na f join global_temp.tripIATA t on t.IATA = f.IATA")
          airports.createOrReplaceGlobalTempView("airports")
          airports.cache()

```
* De même pour **IATA**. On aussi utilisé la fonction **createOrReplaceGlobalTempView** pour créer un tableau temporaire qui va nous servire dans les opérations qui suivent.
3. Ensuite, on définit un autre DataSet **departureDelays_geo** pour Obtenir des attributs clés tels que la *date du vol*, les *retards*, la *distance* et les informations sur les *airports* (origine, destination) comem ceci :
```scala
   val departureDelays_geo = spark.sqlContext.sql("select cast(f.date as int) as tripid, cast(concat(concat(concat(concat(concat(concat
          ('2014-', concat(concat(substr(cast(f.date as string), 1, 2), '-')), substr(cast(f.date as string), 3, 2)), ' '),
          substr(cast(f.date as string), 5, 2)), ':'), substr(cast(f.date as string), 7, 2)), ':00') as timestamp) as `localdate`, cast(f.delay as int),
          cast(f.distance as int), f.origin as src, f.destination as dst, o.city as city_src, d.city as city_dst, o.state as state_src,
          d.state as state_dst from global_temp.departuredelays f join global_temp.airports o on o.iata = f.origin join global_temp.airports d on d.iata = f.destination")
```
4. Et à la fin, on construit nos *Vertice et Edges* comem ceci :  
```scala
    val tripVertices = airports.withColumnRenamed("IATA", "id").distinct()
          val tripEdges = departureDelays_geo.select("tripid", "delay", "src", "dst", "city_dst", "state_dst")

``` 

### Question 3 et 4
#### On crée le **Graphe** on l'affiche et on affiche aussi les Edges et les Vertices : 
```scala
   val tripGraph = GraphFrame(tripVertices, tripEdges)
          println(tripGraph)
// afficher les Edges
    tripVertices.show()
    tripEdges.show()       
```

### Question 5 
#### Sort and display the degree of each vertex 

```scala
     val degree = tripGraph.degrees.sort("degree")
     degree.show()
``` 

### Question 6 
#### Sort and display the indegree (the number of flights to the airport) of each vertex 

```scala
    val inDegree = tripGraph.inDegrees.sort("inDegree")
    inDegree.show()
``` 

### Question 7
#### Sort and display the outdegree (the number of flights leaving the airport) of each vertex
```scala
        val outDegree = tripGraph.outDegrees.sort("outDegree")
        outDegree.show()
``` 

### Question 8 
#### Determine the top transfer airports
* Pour répondre à cette question, nous devons tout d'abord calculer le *degre Ratio* (inDegree/outDegree) en faisant une jointure sur la colonne l'ID : 
```scala
        val degreeRatio = inDegree.join(outDegree, inDegree("id") === outDegree("id")).
          drop(outDegree("id")).
          selectExpr("id", "double(inDegree)/double(outDegree) as degreeRatio").
          cache()

        val transferAirports = degreeRatio.join(airports, degreeRatio("id") === airports("IATA")).
          selectExpr("id", "city", "degreeRatio").
          filter("degreeRatio between 0.9 and 1.1")

        transferAirports.orderBy("degreeRatio").limit(10).show()
``` 

### Question 9 & 10 
#### Compute the triplets & Print the number of airports and trips of your graph

```scala
        tripGraph.triplets.show()

  // 10. 
        println(s"Airports: ${tripGraph.vertices.count()}")
        println(s"Trips: ${tripGraph.edges.count()}"))

``` 

### Question 11
* Pour faire la suite du TP, nosu devons faire quelques configurations pour charger le nouveau fichier en créant un nouvel *DataSet* : 

```scala
       val tripdelaysFilePath2 = "src/main/scala/exemple/data/departuredelays.csv"

        // Obtain departure Delays data
        val departureDelays2 = spark.sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load(tripdelaysFilePath2)
        departureDelays.createOrReplaceGlobalTempView("departureDelays2")
        departureDelays.cache()
```

#### Les requetes : 
1.  Flights departing from **SFO** are most likely to have significant delays

 ```scala
         val sfoDelayedTrips = tripGraph.edges.
                      filter("src = 'SFO' and delay > 0").
                      groupBy("src", "dst").
                      avg("delay").
                      sort("avg(delay)")

                        sfoDelayedTrips.show()

```

2. Destination states tend to have significant delays departing from **SEA** (delay > 10)

```scala
         val seaDelayedTrips = tripGraph.edges.filter("src = 'SEA'").filter("delay > 10")

                        seaDelayedTrips.show()

```

### Question 12
#### Les requetes : 
1. SFO->JAC->SEA

```scala
         val filteredPaths = tripGraph.bfs.fromExpr("id = 'SFO'").toExpr("id = 'JAC'").toExpr("id = 'SEA'").run()
            filteredPaths.show()

```

2. filter query 1 where *delay* <-5. *delay=DEP_DELAY* 
```scala
         val filteredPAthsDelayed = tripGraph.
          find("(a)-[ab]->(b); (b)-[bc]->(c)").
          filter("(a.id = 'SFO') and (ab.delay < -5 or bc.delay < -5) and (b.id = 'JAC') and (c.id = 'SEA')")
```

3. Extract the destinations from **SFO**.

```scala
         val sfoDestinationTrips = tripGraph.edges.
          filter("src = 'SFO'")
            .select("dst")

                sfoDestinationTrips.show()

```