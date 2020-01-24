# Compte rendu Bid Data 
réalisé par le binome : Abdelaziz CHERIFI & Joseph AYEBOU
### Traitement sur le fichier Cf 
#### Question 1
1. on crée un  sc et charge les fichier dans la dans un RDD *line* pour l'utiliser plustard dans le traitement des diffirent fichier de Cf 

```JAVA
    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaRDD<String> lines = sc.textFile("src/cf/*"); 
```    

#### Question 2
1. Tout à bord, on a créé un RDD qui contient tous les mots du fichier spliter avec un espace qui est *wordsFromFile*  en utilisant al fonction ftalMap
```JAVA
    JavaRDD<String> wordsFromFile = lines.flatMap(content -> Arrays.asList(content.split(" ")).iterator());
```
2. Ensuite pour faire le cout, on doit d'abors filtrer en enlevant les **/** et les espace 
```JAVA
    JavaRDD<String> wordsFilter = wordsFromFile.filter(w -> !w.isEmpty()).filter(w->!w.equals("/")).distinct();
```
3. On a utilsié la focntion **count** pour compter les mots sur un RDD de pair en clé/valeur  qu'on a créé avec la fonction *mapToPair* et avec la fonction reduceByKey on fait le compte sur la colonne key 
```JAVA
   JavaPairRDD<String, Long> pairs = wordsFilter.mapToPair(s -> new Tuple2<>(s, 1L));
    JavaPairRDD<String, Long> counts = pairs.reduceByKey((a, b) -> a + b);
    counts.sortByKey(true, 1);
```
4. ensuite on affecte dans uen variable le nombre totale et on l'affiche 
```JAVA
    int totalLength = lineLengths.reduce((a, b) -> a + b);
``` 

#### Question 3
* Tout à bord, on charge le fichier stopWords dans un nouvel RDD _stopFrenchW_ ensuite puisque les RDDs de stopWords et des fichier du dossier Cf sont en structurés en colonnne on peut directement utilsier la fonction **subtract** pour enlever les stopWords des mots des autres fichiers 
```JAVA
    JavaRDD<String> stopFrenchW = sc.textFile("src/test/Frenchstopwords.txt") ;
    JavaRDD<String> wordSubstracted = wordsFromFile.filter(w -> !w.isEmpty()).filter(w->!w.equals("/")).subtract(stopFrenchW);
``` 

#### Question 4 
* Pour afficher les top 10 mots les plus répétés dans les fichiers, on doit d'abord inverser dnas un novel RDD la liste pair qui est dans le RDD **wordSubstracted** par ce que dans la deuxième colonne on a la valeur de chaque mot de combien il est répété dans la totalité des fichier, ensuite on les trie et on commance le calcul à partir du bas car le tri est par ordre croissant des valeurs 
```JAVA
    JavaPairRDD<String, Long> pairsWords = wordSubstracted.mapToPair(s -> new Tuple2<>(s, 1L));
    JavaPairRDD<String, Long> countWords = pairsWords.reduceByKey((a, b) -> a + b);
    JavaPairRDD<Long, String> pairsW = countWords.mapToPair(s -> new Tuple2<Long,String>(s._2,s._1)).sortByKey(false,1);
``` 

#### Question 5 
1. Dans cetet partie du TP, nous devons tout à bord sauvegarder tous les ficheirs dans un seul RDD, car avant chaque fichier était dans un RDD à part. Maintenant notre RDD contient la totalité des fichiers. et ça à l'aide de la fonction _wholeTextFiles_
```JAVA
JavaPairRDD<String, String> StopFrWords = sc.wholeTextFiles("src/test/Frenchstopwords.txt");
``` 
2. Ensuite, on construit un liste de type _Row_ pour structurer chaque fichier dans une seule ligne, on finale le nombre de ligne de notre liste contient exactement le nombre de fichiers de Cf
```JAVA
	ArrayList<Row> dataset =  new ArrayList<Row>();
    
    	for (Tuple2<String, String> s : data.collect()) {
    	
        dataset.add(RowFactory.create(Arrays.asList(s._2.replaceAll("\\s", " ").split(" "))));
       
        	}
``` 
..* Donc la, en sauvegarde que la deuxième colonne, car la premère était le chemin relatif du fichier, et on a chaque ligne qui est une transaction correspond a un fichier complet avec tous les mots de ce fichier. Cette démarche nous aidera par la suite pour faire la soustraction des mots.


#### Question 6
* La quesion 6, c'est la question qui nous a pris le plus de temps, et pour faire la soustraction des mots du fichier stopFrechWords, on a utiliser la fonction _transform_ de **StopWordsRemover** sur une **Dataset** de type Row

#### Question 7 
* On affiche la fréquance des items en faisant varié la valeur de le seuil minimum _minsup_ qu ireprésente  le pourcentage de textes où la régle est apparait, pour cela, on utilise le module **FPGrowth**  sur lequel on fixe la valeur du support pour étudier l'extraction d'éléments fréqents 
 ```JAVA
 FPGrowth fpg = new FPGrowth()
    	    		  .setMinSupport(0.8) // on change ici les valeurs de minsup :0.2, 0.3, 0.5, 0.8 
    	    		  .setNumPartitions(totalLength);

```

#### Question 8 & 10 Non traité 

#### Question 9
* à l'aide de **FPGrowth** on peut tester les valeurs du  le pourcentage de fois où la régle est vérifiée  _minconf_ sur laquel on fixe à chaque fois la valeur de _minconf_ pour étudier l'extraction d'éléments fréqents 
 ```JAVA
 double minConfidence = 0.8; // ici on modifie les vaeurs de minconf pour avoir les # résultats : 0.2, 0.5, 0.8
    	    	    for (AssociationRules.Rule<String> rule
    	    	      : model.generateAssociationRules(minConfidence).toJavaRDD().collect()) {
    	    	      System.out.println(
    	    	        rule.javaAntecedent() + " => " + rule.javaConsequent() + ", " + rule.confidence());
    	    	    }


```

#### Question 11 
* Dans cette partie, on refait les même démarches mais seulement cette fois ci c'est avec les fichiers du dossier Cp au lieu de Cf