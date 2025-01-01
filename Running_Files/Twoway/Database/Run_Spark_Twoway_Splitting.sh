#!/usr/bin/env bash
if [ -d "/home/uhartegr/Documents/Ecole_Ingé_Enssat/Grèce/Projet/Code/fuzzy-join/fuzzy-join/OUTPUT/Database/Twoway/Splitting" ]; then
    rm -R /home/uhartegr/Documents/Ecole_Ingé_Enssat/Grèce/Projet/Code/fuzzy-join/fuzzy-join/OUTPUT/Database/Twoway/Splitting
fi
/usr/local/spark/bin/spark-submit --class Database.twoway.Splitting /home/uhartegr/Documents/Ecole_Ingé_Enssat/Grèce/Projet/Code/fuzzy-join/fuzzy-join/target/thesis-1.0-SNAPSHOT.jar /home/uhartegr/Documents/Ecole_Ingé_Enssat/Grèce/Projet/Code/fuzzy-join/fuzzy-join/Dataset/Database/Dataset_1.txt /home/uhartegr/Documents/Ecole_Ingé_Enssat/Grèce/Projet/Code/fuzzy-join/fuzzy-join/Dataset/Database/Dataset_2.txt /home/uhartegr/Documents/Ecole_Ingé_Enssat/Grèce/Projet/Code/fuzzy-join/fuzzy-join/OUTPUT/Database/Twoway/Splitting  2 1
