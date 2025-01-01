#!/usr/bin/env bash
if [ -f "/home/uhartegr/Documents/Ecole_Ingé_Enssat/Grèce/Projet/Code/fuzzy-join/fuzzy-join/OUTPUT/JSON/DatasetJsonReqPath.txt" ]; then
  rm /home/uhartegr/Documents/Ecole_Ingé_Enssat/Grèce/Projet/Code/fuzzy-join/fuzzy-join/OUTPUT/JSON/DatasetJsonReqPath.txt
fi 
/usr/local/spark/bin/spark-submit --class selfjoins.JsonReqPath --jars /home/uhartegr/Documents/Ecole_Ingé_Enssat/Grèce/Projet/Code/fuzzy-join/fuzzy-join/target/thesis-1.0-SNAPSHOT.jar,/home/uhartegr/Desktop/jsonpath-1.2.jar / /home/uhartegr/Documents/Ecole_Ingé_Enssat/Grèce/Projet/Code/fuzzy-join/fuzzy-join/Dataset/JSON/Dataset_1.json /home/uhartegr/Documents/Ecole_Ingé_Enssat/Grèce/Projet/Code/fuzzy-join/fuzzy-join/OUTPUT/JSON/DatasetJsonReqPath.txt $.book.information.character[*].sexe
