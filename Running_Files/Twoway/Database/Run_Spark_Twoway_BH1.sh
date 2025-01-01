#!/usr/bin/env bash
if [ -d "/home/uhartegr/Documents/Ecole_Ingé_Enssat/Grèce/Projet/Code/fuzzy-join/fuzzy-join/OUTPUT/Database/Twoway/BH1" ]; then
    rm -R /home/uhartegr/Documents/Ecole_Ingé_Enssat/Grèce/Projet/Code/fuzzy-join/fuzzy-join/OUTPUT/Database/Twoway/BH1
fi
/usr/local/spark/bin/spark-submit --class Database.twoway.BH1 /home/uhartegr/Documents/Ecole_Ingé_Enssat/Grèce/Projet/Code/fuzzy-join/fuzzy-join/target/thesis-1.0-SNAPSHOT.jar /home/uhartegr/Documents/Ecole_Ingé_Enssat/Grèce/Projet/Code/fuzzy-join/fuzzy-join/Dataset/Database/Dataset_1.txt /home/uhartegr/Documents/Ecole_Ingé_Enssat/Grèce/Projet/Code/fuzzy-join/fuzzy-join/Dataset/Database/Dataset_2.txt /home/uhartegr/Documents/Ecole_Ingé_Enssat/Grèce/Projet/Code/fuzzy-join/fuzzy-join/OUTPUT/Database/Twoway/BH1 /home/uhartegr/Documents/Ecole_Ingé_Enssat/Grèce/Projet/Code/fuzzy-join/fuzzy-join/Vocabulary/Database/vocabulary_Twoway_BH1.txt  2 1 0
