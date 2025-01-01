#!/usr/bin/env bash

: <<'JsonReqPath'
spark-submit --class JSON.Main --jars ./target/thesis-1.0-SNAPSHOT.jar,/home/uhartegr/Desktop/jsonpath-1.2.jar / \
    ./Dataset/JSON/Dataset_1.json \
    null \
    $.book.information.character[*].sexe \
    null \
    /home/uhartegr/Documents/Ecole_Ingé_Enssat/Grèce/Projet/Code/fuzzy-join/fuzzy-join/OUTPUT/JSON/Main/Selfjoin/ \
    null \
    JsonReqPath \
    1 \
    2
JsonReqPath

: <<'Naive_Selfjoin'
spark-submit --class JSON.Main --jars ./target/thesis-1.0-SNAPSHOT.jar,C:/Users/m1361/Desktop/json-path-1.2.0.jar / \
./Dataset/JSON/Dataset_1.json \
null \
$.book.information.character[*].sexe \
null \
./OUTPUT/JSON/Main/Selfjoin/ \
null \
Naive \
1 \
-1
Naive_Selfjoin

: <<'BH1_Selfjoin'
spark-submit --class JSON.Main --jars /home/uhartegr/Documents/Ecole_Ingé_Enssat/Grèce/Projet/Code/fuzzy-join/fuzzy-join/target/thesis-1.0-SNAPSHOT.jar,/home/uhartegr/Desktop/jsonpath-1.2.jar / \
/home/uhartegr/Documents/Ecole_Ingé_Enssat/Grèce/Projet/Code/fuzzy-join/fuzzy-join/Dataset/JSON/Dataset_1.json \
null \
$.book.information.character[*].sexe \
null \
/home/uhartegr/Documents/Ecole_Ingé_Enssat/Grèce/Projet/Code/fuzzy-join/fuzzy-join/OUTPUT/JSON/Main/Selfjoin/ \
/home/uhartegr/Documents/Ecole_Ingé_Enssat/Grèce/Projet/Code/fuzzy-join/fuzzy-join/Vocabulary/JSON/vocabulary_Selfjoin_BH1.txt \
BH1 \
1 \
0
BH1_Selfjoin

: <<'Splitting_Selfjoin'
/usr/local/spark/bin/spark-submit --class JSON.Main --jars /home/uhartegr/Documents/Ecole_Ingé_Enssat/Grèce/Projet/Code/fuzzy-join/fuzzy-join/target/thesis-1.0-SNAPSHOT.jar,/home/uhartegr/Desktop/jsonpath-1.2.jar / \
/home/uhartegr/Documents/Ecole_Ingé_Enssat/Grèce/Projet/Code/fuzzy-join/fuzzy-join/Dataset/JSON/Dataset_1.json \
null \
$.book.information.character[*].sexe \
null \
/home/uhartegr/Documents/Ecole_Ingé_Enssat/Grèce/Projet/Code/fuzzy-join/fuzzy-join/OUTPUT/JSON/Main/Selfjoin/ \
null \
Splitting \
1 \
-1
Splitting_Selfjoin

: <<'Vocabulary_Selfjoin'
/usr/local/spark/bin/spark-submit --class JSON.Main --jars /home/uhartegr/Documents/Ecole_Ingé_Enssat/Grèce/Projet/Code/fuzzy-join/fuzzy-join/target/thesis-1.0-SNAPSHOT.jar,/home/uhartegr/Desktop/jsonpath-1.2.jar / \
/home/uhartegr/Documents/Ecole_Ingé_Enssat/Grèce/Projet/Code/fuzzy-join/fuzzy-join/Dataset/JSON/Dataset_1.json \
null \
$.book.information.character[*].sexe \
null \
/home/uhartegr/Documents/Ecole_Ingé_Enssat/Grèce/Projet/Code/fuzzy-join/fuzzy-join/OUTPUT/JSON/Main/Selfjoin/ \
/home/uhartegr/Documents/Ecole_Ingé_Enssat/Grèce/Projet/Code/fuzzy-join/fuzzy-join/Vocabulary/JSON/vocabulary_Selfjoin.txt \
Vocabulary \
1 \
-1
Vocabulary_Selfjoin

: <<'Naive_Twoway'
spark-submit --class JSON.Main --jars ./target/thesis-1.0-SNAPSHOT.jar,file:///C:/Users/m1361/Desktop/json-path-1.2.0.jar / \
/home/uhartegr/Documents/Ecole_Ingé_Enssat/Grèce/Projet/Code/fuzzy-join/fuzzy-join/Dataset/JSON/Dataset_1.json \
/home/uhartegr/Documents/Ecole_Ingé_Enssat/Grèce/Projet/Code/fuzzy-join/fuzzy-join/Dataset/JSON/Dataset_2.json \
$.book.information.character[*].sexe \
$.book.information.character[*].sexe \
/home/uhartegr/Documents/Ecole_Ingé_Enssat/Grèce/Projet/Code/fuzzy-join/fuzzy-join/OUTPUT/JSON/Main/Twoway/ \
null \
Naive \
1 \
-1
Naive_Twoway

: <<'BH1_Twoway'

BH1_Twoway

: <<'Splitting_Twoway'
/usr/local/spark/bin/spark-submit --class JSON.Main --jars /home/uhartegr/Documents/Ecole_Ingé_Enssat/Grèce/Projet/Code/fuzzy-join/fuzzy-join/target/thesis-1.0-SNAPSHOT.jar,/home/uhartegr/Desktop/jsonpath-1.2.jar / \
/home/uhartegr/Documents/Ecole_Ingé_Enssat/Grèce/Projet/Code/fuzzy-join/fuzzy-join/Dataset/JSON/Dataset_1.json \
/home/uhartegr/Documents/Ecole_Ingé_Enssat/Grèce/Projet/Code/fuzzy-join/fuzzy-join/Dataset/JSON/Dataset_2.json \
$.book.information.character[*].sexe \
$.book.information.character[*].sexe \
/home/uhartegr/Documents/Ecole_Ingé_Enssat/Grèce/Projet/Code/fuzzy-join/fuzzy-join/OUTPUT/JSON/Main/Twoway/ \
null \
Splitting \
1 \
-1
Splitting_Twoway

: <<'Vocabulary_Twoway'
spark-submit --class JSON.Main --jars /home/uhartegr/Documents/Ecole_Ingé_Enssat/Grèce/Projet/Code/fuzzy-join/fuzzy-join/target/thesis-1.0-SNAPSHOT.jar,/home/uhartegr/Desktop/jsonpath-1.2.jar / \
/home/uhartegr/Documents/Ecole_Ingé_Enssat/Grèce/Projet/Code/fuzzy-join/fuzzy-join/Dataset/JSON/Dataset_1.json \
/home/uhartegr/Documents/Ecole_Ingé_Enssat/Grèce/Projet/Code/fuzzy-join/fuzzy-join/Dataset/JSON/Dataset_2.json \
$.book.information.character[*].sexe \
$.book.information.character[*].sexe \
/home/uhartegr/Documents/Ecole_Ingé_Enssat/Grèce/Projet/Code/fuzzy-join/fuzzy-join/OUTPUT/JSON/Main/Twoway/ \
/home/uhartegr/Documents/Ecole_Ingé_Enssat/Grèce/Projet/Code/fuzzy-join/fuzzy-join/Vocabulary/JSON/vocabulary_Twoway.txt \
Vocabulary \
1 \
-1
Vocabulary_Twoway

spark-submit --class JSON.Main --master local[8] --jars ".\target\thesis-1.0-SNAPSHOT-jar-with-dependencies.jar",".\json-path-1.2.0.jar" / "./Dataset/JSON/Dataset_1.json" null $.book.information.character[*].sexe null "./OUTPUT/" null Naive 1 -1

spark-submit --class JSON.Main --master local[8] --jars ".\target\thesis-1.0-SNAPSHOT-jar-with-dependencies.jar",".\json-path-1.2.0.jar" / "./Dataset/JSON/Dataset_1.json" "./Dataset/JSON/Dataset_2.json" $.book.information.character[*].sexe $.book.information.character[*].sexe "./OUTPUT/" null Naive 1 -1

spark-submit --class JSON.Main --master local[8] --jars ".\target\thesis-1.0-SNAPSHOT-jar-with-dependencies.jar",".\json-path-1.2.0.jar" / "./Dataset/JSON/Dataset_1.json" "./Dataset/JSON/Dataset_2.json" $.book.information.character[*].sexe $.book.information.character[*].sexe "./OUTPUT/" "./Vocabulary/JSON/vocabulary_Twoway_BH1.txt" BH1 1 0

spark-submit --class JSON.Main --master local[8] --jars ".\target\thesis-1.0-SNAPSHOT-jar-with-dependencies.jar",".\json-path-1.2.0.jar" / "./Dataset/JSON/Dataset_1.json" null $.book.information.character[*].sexe null "./OUTPUT/" "./Vocabulary/JSON/vocabulary_Selfjoin_BH1.txt" BH1 1 0

: <<'Post_Simil'
spark-submit --class StructSimil_PostJson.Main --master local[8] --jars ".\target\thesis-1.0-SNAPSHOT-jar-with-dependencies.jar",".\json-path-1.2.0.jar" / "./Dataset/JSON/Dataset_1.json" null $.book.information.character[*].sexe null "./OUTPUT/" null Naive Naive 1 Index Naive -1

Post_Simil




spark-submit --class StructSimil.Main_Struct_Simil --master <master-url> path/to/your-uber-jar.jar / <path_to_dataset_1> <path_to_dataset_2> <jsonpath_request_1> <jsonpath_request_2> <path_to_output> <distance_thresold> <graph_creation_mode> <similarity_compute_mode>

spark-submit --class StructSimil.Main_Struct_Simil --master local[8] --jars ".\target\thesis-1.0-SNAPSHOT-jar-with-dependencies.jar",".\json-path-1.2.0.jar" / "./Dataset/JSON/Dataset_1.json" null $.book.information.character[*].sexe null "./OUTPUT/" 1 Index Naive

StructSimil.Main_Struct_Simil "../../Dataset/JSON/Dataset_1.json" null $.book.information.character[*].sexe null "../../OUTPUT/" 1 Index Naive