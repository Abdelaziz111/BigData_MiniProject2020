package test.test;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

//import scala.collection.immutable.List;
import scala.collection.parallel.ParIterableLike.Foreach;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;

//import org.apache.spark.ml.fpm.FPGrowth;
//import org.apache.spark.ml.fpm.FPGrowthModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
import org.apache.spark.mllib.fpm.AssociationRules;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;
import org.apache.spark.ml.feature.StopWordsRemover;
//import org.apache.spark.ml.feature.StopWordsRemover;
//import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.Row;
//import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.*;




/**
 * Hello world!
 *
 */
public class AppTest
{
    public static void main( String[] args )
    {
    /**
     * 1-Load Document in Fc
     */
    	
    SparkConf conf = new SparkConf().setAppName("BigData").setMaster("local");
    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaRDD<String> lines = sc.textFile("src/cf/*");
   
   /**
    * 2- Count, sort and display the number of words in all documents
    */
    JavaRDD<String> wordsFromFile = lines.flatMap(content -> Arrays.asList(content.split(" ")).iterator());
   
    JavaRDD<String> wordsFilter = wordsFromFile.filter(w -> !w.isEmpty()).filter(w->!w.equals("/")).distinct();
    System.out.println("Count = >" + wordsFilter.count());
    JavaRDD<String> wordFilterSort = wordsFilter.sortBy(new Function<String, String>() {
     private static final long serialVersionUID = 1L;

     @Override
     public String call( String value ) throws Exception {
       return value;}
   		}, true, 1);
   /**
    * afficher le contenu de Count, sort and display the number of words in all documents
    */
    /*
    for (String s : wordFilterSort.collect()) {
    	System.out.println(s);
    		}
    		*/
    
    JavaPairRDD<String, Long> pairs = wordsFilter.mapToPair(s -> new Tuple2<>(s, 1L));
    JavaPairRDD<String, Long> counts = pairs.reduceByKey((a, b) -> a + b);
    counts.sortByKey(true, 1);
   
    

    
    JavaRDD<Integer> lineLengths = lines.map(s -> s.length());
    int totalLength = lineLengths.reduce((a, b) -> a + b);
   // display nomber of words 
    System.out.println("-----------------------------------");
    System.out.println("Le total des mot est : <===="+ totalLength+"====>" );
    
    
    /** 
     * 3 remove frechStopWords 
     */
    
    JavaRDD<String> stopFrenchW = sc.textFile("src/test/Frenchstopwords.txt") ;
    JavaRDD<String> wordSubstracted = wordsFromFile.filter(w -> !w.isEmpty()).filter(w->!w.equals("/")).subtract(stopFrenchW);
    
   /* 1 System.out.println("Afficher les mots après substraction... "+wordSubstracted.count()+" comparé avec avant la substraction .."+wordsFromFile.count()); */
    
    
  /**
   *  4 display top 10 of the words 
   */
    JavaPairRDD<String, Long> pairsWords = wordSubstracted.mapToPair(s -> new Tuple2<>(s, 1L));
    JavaPairRDD<String, Long> countWords = pairsWords.reduceByKey((a, b) -> a + b); /*sortBy((new Function<String, String>() {
        private static final long serialVersionUID = 1L;

        @Override
        public String call( String value ) throws Exception {
          return value;
        }
       }), true, 1);
       */
    JavaPairRDD<Long, String> pairsW = countWords.mapToPair(s -> new Tuple2<Long,String>(s._2,s._1)).sortByKey(false,1);
  /* 2  pairsW.foreach(s -> {System.out.println("....all Words.."+s._1()+"..."+s._2());}); */
    //JavaPairRDD<Long, String> countWordsInv = countWords.map(s ->  s._2(),s._1()); 
    
    //countWords.foreach(s-> { System.out.println("......all Words.."+" "+ s._1()+" "+ s._2()+".....");});
    /* Affichage les 10 premiers mots les plus répétés*/  
    //pairsW.take(10).forEach(s-> { System.out.println("..top Words.."+s._1+".."+s._2);});*/
   
  /**
   * JavaRDD<String> data = sc.textFile("src/cf/cf2010.txt");
    JavaRDD<List<String>> transactions = data.map(line -> Arrays.asList(line.split(" ")));
    JavaRDD<List<String>> tranStop = french.map(line -> Arrays.asList(line.split(" ")));
    transactions.subtract(tranStop);
    transactions.collect().forEach(a -> System.out.println(a));
   
   */
    
     
     
    

    
    
    
    /**
     * question 6	afficher les transactions et construire les transations
     */
    
    JavaPairRDD<String, String> data = sc.wholeTextFiles("src/cf/*");
    JavaPairRDD<String, String> StopFrWords = sc.wholeTextFiles("src/test/Frenchstopwords.txt");
    
    /**
     * System.out.println(".............notre transaction.............");
     */
    
    /**
     *  5 for (Tuple2<String, String> s : data.collect()) {
     *  System.out.println("trans......"+s);
    	}
     */
    
    	
    
    
    //JavaRDD<List<String>> StpFrWordsTr = StopFrWords.map(line -> Arrays.asList(line._2.split(" ")));
   /* 
    for (List<String> s : StpFrWordsTr.collect()) {
        System.out.println("....transWords......"+s);
        	}
    */    	
    
   
   
   // JavaRDD<List<String>> wordSubstractedTrans = transactions.filter(w->!w.equals(",")).filter(w -> !w.isEmpty()).filter(w->!w.equals("/")).subtract(StpFrWordsTr) ;
  
    
    ArrayList<Row> dataset =  new ArrayList<Row>();
    
    for (Tuple2<String, String> s : data.collect()) {
    	
        dataset.add(RowFactory.create(Arrays.asList(s._2.replaceAll("\\s", " ").split(" "))));
       
        	}
 
 
 //   Stopwords.forEach(s -> {System.out.println("...transTranch......"+s);});
    
   dataset.forEach(s -> {System.out.println("...transTranchAvantRet......"+s);});
   //data.foreachAsync(s -> {System.out.println("...transTranchAvantRetttttttttttt......"+s);});	

     
     StopWordsRemover remover = new StopWordsRemover()
    	      .setInputCol("raw")
    	      .setOutputCol("filtered");
    
     
     SparkSession spark = SparkSession
             .builder()
             .config(conf)
             .getOrCreate();
     
    	    StructType schema = new StructType(new StructField[]{
    	    		new StructField("raw", DataTypes.createArrayType(DataTypes.StringType), false, Metadata.empty())});

    	    Dataset<Row> datasetR = spark.createDataFrame(dataset, schema);
    	    remover.transform(datasetR).show(false);
    	    datasetR.foreach(s -> {System.out.println("...transTranchAprès......"+s);});
    	    
    	    
    	    /* question 7 */
    	    
    	    JavaRDD<List<String>> transactions = data.map(line -> Arrays.asList(line._2.split(" ")));
    	    
    	    FPGrowth fpg = new FPGrowth()
    	    		  .setMinSupport(0.8) // on change ici les valeurs de minsup :0.2, 0.3, 0.5, 0.8 
    	    		  .setNumPartitions(totalLength);
    	    	
    	    		FPGrowthModel<String> model = fpg.run(transactions);

    	    		for (FPGrowth.FreqItemset<String> itemset: model.freqItemsets().toJavaRDD().collect()) {
    	    		  System.out.println("FrequentElm = "+"[" + itemset.javaItems() + "], " + itemset.freq());
    	    		}
    	    
    	    		
    	    		
    	    		/* question 9 */ 
    	    		
    	    		double minConfidence = 0.8; // ici on modifie les vaeurs de minconf pour avoir les # résultats : 0.2, 0.5, 0.8
    	    	    for (AssociationRules.Rule<String> rule
    	    	      : model.generateAssociationRules(minConfidence).toJavaRDD().collect()) {
    	    	      System.out.println(
    	    	        rule.javaAntecedent() + " => " + rule.javaConsequent() + ", " + rule.confidence());
    	    	    }
    
    	    	    
    	    	    
    	    	    /**********************************************
    	    	     * Question 10 pour le fichier cp 
    	    	     */
    	    	    
    	    	    
    	    	    
    	    	    
    	    	    
    	    	    
    	    	    
    	    	    
    	    	    
    	    	  //1-Load
    	    	  //SparkConf conf = new SparkConf().setAppName("BigData").setMaster("local");
    	    	    //JavaSparkContext sc = new JavaSparkContext(conf);
    	    	    JavaRDD<String> linesc = sc.textFile("src/cp/*");
    	    	   
    	    	    //2- count / sort / display
    	    	    JavaRDD<String> wordsFromFilec = lines.flatMap(content -> Arrays.asList(content.split(" ")).iterator());
    	    	   
    	    	    JavaRDD<String> wordsFilterc = wordsFromFilec.filter(w -> !w.isEmpty()).filter(w->!w.equals("/")).distinct();
    	    	    System.out.println("Count = >" + wordsFilterc.count());
    	    	    JavaRDD<String> wordFilterSortc = wordsFilterc.sortBy(new Function<String, String>() {
    	    	     private static final long serialVersionUID = 1L;

    	    	     @Override
    	    	     public String call( String value ) throws Exception {
    	    	       return value;
    	    	     }
    	    	    }, true, 1);
    	    	   
    	    	    for (String s : wordFilterSortc.collect()) {
    	    	System.out.println(s);
    	    	}
    	    	    JavaPairRDD<String, Long> pairsc = wordsFilterc.mapToPair(s -> new Tuple2<>(s, 1L));
    	    	    JavaPairRDD<String, Long> countsc = pairsc.reduceByKey((a, b) -> a + b);
    	    	    counts.sortByKey(true, 1);
    	    	   
    	    
    	    	    JavaRDD<Integer> lineLengthsc = linesc.map(s -> s.length());
    	    	    int totalLengthc = lineLengthsc.reduce((a, b) -> a + b);
    	    	   
    	    	    System.out.println("-----------------------------------");
    	    	    System.out.println("Le total est : ========>" + totalLength);
    	    	    // 3 remove frech stop 
    	    	    JavaRDD<String> stopFrenchWc = sc.textFile("src/test/Frenchstopwords.txt") ;
    	    	    JavaRDD<String> wordSubstractedc = wordsFromFile.filter(w -> !w.isEmpty()).filter(w->!w.equals("/")).subtract(stopFrenchW);
    	    	    
    	    	   /* 1 System.out.println("Afficher les mots après substraction... "+wordSubstracted.count()+" comparé avec avant la substraction .."+wordsFromFile.count()); */
    	    	    
    	    	    
    	    	    // 4 display top 10 of the words 
    	    	    JavaPairRDD<String, Long> pairsWordsc = wordSubstractedc.mapToPair(s -> new Tuple2<>(s, 1L));
    	    	    JavaPairRDD<String, Long> countWordsc = pairsWordsc.reduceByKey((a, b) -> a + b); /*sortBy((new Function<String, String>() {
    	    	        private static final long serialVersionUID = 1L;

    	    	        @Override
    	    	        public String call( String value ) throws Exception {
    	    	          return value;
    	    	        }
    	    	       }), true, 1);
    	    	       */
    	    	    JavaPairRDD<Long, String> pairsWc = countWordsc.mapToPair(s -> new Tuple2<Long,String>(s._2,s._1)).sortByKey(false,1);
    	    	  /* 2  pairsW.foreach(s -> {System.out.println("....all Words.."+s._1()+"..."+s._2());}); */
    	    	    //JavaPairRDD<Long, String> countWordsInv = countWords.map(s ->  s._2(),s._1()); 
    	    	    
    	    	    //countWords.foreach(s-> { System.out.println("......all Words.."+" "+ s._1()+" "+ s._2()+".....");});
    	    	    // /* faire l'affichage des 10 premiers*/  pairsW.take(10).forEach(s-> { System.out.println("..top Words.."+s._1+".."+s._2);});*/
    	    	   
    	    	    // 5 
    	    	    /*
    	    	     *  JavaRDD<String> data = sc.textFile("src/cf/cf2010.txt");
    	    	    JavaRDD<List<String>> transactions = data.map(line -> Arrays.asList(line.split(" ")));
    	    	    JavaRDD<List<String>> tranStop = french.map(line -> Arrays.asList(line.split(" ")));
    	    	    transactions.subtract(tranStop);
    	    	    transactions.collect().forEach(a -> System.out.println(a));
    	    	   
    	    	     * 
    	    	     */
    	    	    

    	    	    
    	    	    
    	    	    
    	    	    // question 6	afficher les transactions et construire les transations
    	    	    
    	    	    JavaPairRDD<String, String> datac = sc.wholeTextFiles("src/cp/*");
    	    	    JavaPairRDD<String, String> StopFrWordsc = sc.wholeTextFiles("src/test/Frenchstopwords.txt");
    	    	    System.out.println(".............notre transaction.............");
    	    	    
    	    	    /* 5 for (Tuple2<String, String> s : data.collect()) {
    	    	    System.out.println("trans......"+s);
    	    	    	}
    	    	    	*/
    	    	    
    	    	    
    	    	    //JavaRDD<List<String>> StpFrWordsTr = StopFrWords.map(line -> Arrays.asList(line._2.split(" ")));
    	    	   /* 
    	    	    for (List<String> s : StpFrWordsTr.collect()) {
    	    	        System.out.println("....transWords......"+s);
    	    	        	}
    	    	    */    	
    	    	    
    	    	    System.out.println("...transTranch......");
    	    	    System.out.println("...transTranch......");
    	    	    System.out.println("...transTranch......");
    	    	   
    	    	   // JavaRDD<List<String>> wordSubstractedTrans = transactions.filter(w->!w.equals(",")).filter(w -> !w.isEmpty()).filter(w->!w.equals("/")).subtract(StpFrWordsTr) ;
    	    	  
    	    	    
    	    	    ArrayList<Row> datasetc =  new ArrayList<Row>();
    	    	    
    	    	    for (Tuple2<String, String> s : datac.collect()) {
    	    	    	
    	    	        datasetc.add(RowFactory.create(Arrays.asList(s._2.replaceAll("\\s", " ").split(" "))));
    	    	       
    	    	        	}
    	    	 
    	    	 
    	    	 //   Stopwords.forEach(s -> {System.out.println("...transTranch......"+s);});
    	    	    
    	    	   datasetc.forEach(s -> {System.out.println("...transTranchAvantRet......"+s);});
    	    	   //data.foreachAsync(s -> {System.out.println("...transTranchAvantRetttttttttttt......"+s);});	

    	    	     
    	    	     StopWordsRemover removerc = new StopWordsRemover()
    	    	    	      .setInputCol("raw")
    	    	    	      .setOutputCol("filtered");
    	    	    
    	    	     

    	    	     SparkSession sparkc = SparkSession
    	    	             .builder()
    	    	             .config(conf)
    	    	             .getOrCreate();
    	    	     
    	    	    	    StructType schemac = new StructType(new StructField[]{
    	    	    	    		new StructField("raw", DataTypes.createArrayType(DataTypes.StringType), false, Metadata.empty())});

    	    	    	    Dataset<Row> datasetRc = spark.createDataFrame(datasetc, schema);
    	    	    	    removerc.transform(datasetRc).show(false);
    	    	    	    datasetRc.foreach(s -> {System.out.println("...transTranchAprès......"+s);});
    	    	    	    
    	    	    	    
    	    	    	    /* question 7 */
    	    	    	    
    	    	    	    JavaRDD<List<String>> transactionsc = data.map(line -> Arrays.asList(line._2.split(" ")));
    	    	    	    
    	    	    	    FPGrowth fpgc = new FPGrowth()
    	    	    	    		  .setMinSupport(0.8) // on change ici les valeurs de minsup :0.2, 0.3, 0.5, 0.8 
    	    	    	    		  .setNumPartitions(totalLength);
    	    	    	    	
    	    	    	    		FPGrowthModel<String> modelc = fpg.run(transactionsc);

    	    	    	    		for (FPGrowth.FreqItemset<String> itemset: model.freqItemsets().toJavaRDD().collect()) {
    	    	    	    		  System.out.println("Frequence = "+"[" + itemset.javaItems() + "], " + itemset.freq());
    	    	    	    		}
    	    	    	    
    	    	    	    		
    	    	    	    		
    	    	    	    		/* question 9 */ 
    	    	    	    		
    	    	    	    		double minConfidencec = 0.8; // ici on modifie les vaeurs de minconf pour avoir les # résultats : 0.2, 0.5, 0.8
    	    	    	    	    for (AssociationRules.Rule<String> rule
    	    	    	    	      : model.generateAssociationRules(minConfidencec).toJavaRDD().collect()) {
    	    	    	    	      System.out.println(
    	    	    	    	        rule.javaAntecedent() + " => " + rule.javaConsequent() + ", " + rule.confidence());
    	    	    	    	    }
    	    	    
    	    	    
    	    	    
    	    	    
    	    	    
    	    	    
    	    	    
    	    	    
    	    	    
    	    	    
    
    }
}
