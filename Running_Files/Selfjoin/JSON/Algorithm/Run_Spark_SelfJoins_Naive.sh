#!/usr/bin/env bash
if [ -d "/home/uhartegr/Documents/Ecole_Ingé_Enssat/Grèce/Projet/Code/fuzzy-join/fuzzy-join/OUTPUT/JSON/Selfjoin/Naive" ]; then
    rm -R /home/uhartegr/Documents/Ecole_Ingé_Enssat/Grèce/Projet/Code/fuzzy-join/fuzzy-join/OUTPUT/JSON/Selfjoin/Naive
fi

/usr/local/spark/bin/spark-submit --class JSON.selfjoins.Naive /home/uhartegr/Documents/Ecole_Ingé_Enssat/Grèce/Projet/Code/fuzzy-join/fuzzy-join/target/thesis-1.0-SNAPSHOT.jar /home/uhartegr/Documents/Ecole_Ingé_Enssat/Grèce/Projet/Code/fuzzy-join/fuzzy-join/Dataset/JSON/Dataset_1.txt /home/uhartegr/Documents/Ecole_Ingé_Enssat/Grèce/Projet/Code/fuzzy-join/fuzzy-join/OUTPUT/JSON/Selfjoin/Naive 0 1
