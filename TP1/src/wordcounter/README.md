TP1
===

Création des paires de mots :

*hadoop jar WordCount.jar wordcounter.WordCount /data/aol/AOL-01.txt WCountPairAOL*

Création du top 5 :

*hadoop jar WordCount.jar wordcounter.Toper WCountPairAOL/part-r-00000 Top5*

Afficher le résultat :

*hdfs dfs -cat Top5/part-r-00000*