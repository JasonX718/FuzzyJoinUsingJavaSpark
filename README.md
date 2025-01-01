# Fuzzy-Join on Tree-structured data

## Table of Contents

<summary>
    <ol>
        <li><a href="#description">Description</a></li>
        <li><a href="#motivation">Motivation</a></li>
        <li><a href="#sample">Sample</a></li>
            <ol>
                <li><a href="#input">Input</a></li>
                <li><a href="#json-request-path">JSON Request Path</a></li>
                <li><a href="#fuzzy-join-algorithm">Fuzzy Join Algorithm</a></li>
            </ol>
        <li><a href="#prerequisites">Prerequisites</a></li>
        <li><a href="#usage">Usage</a></li>
        <li><a href="#support">Support</a></li>
        <li><a href="#roadmap">Roadmap</a></li>
        <li><a href="#authors-and-acknowledgment">Authors and acknowledgment</a></li>
        <li><a href="#glossary">Glossary</a></li>
        <li><a href="#project-status">Project status</a></li>
    </ol>
<br>    

## Description
This project provides scalable fuzzy join computations on large structured graph databases.
The project is implemented in Java, based on Spark with MapReduce and HDFS. The project runs the tasks in parallel and the data is distributed.
This project is linked to [thesis](https://gitlab.com/fuzzyjoins/thesis), which allows to do the same with large classical databases.

## Motivation
To know if in structured graph databases there are  <a href="#similarity">Similarities</a> between the values.
To join each similar graph, to build a new merge graph containing both graphs.
To delete a version of each similar graph, to reduce the size of your database.

## Sample
<br>

### Input

There are one or two files, database extracts of structured graphs that need to be test. In order to know if there are similarities between values in your graphs and then be able to join them or process them.

<br>

**file_1.json** :
```json
{"id": 1,"name": "John","age": 25,"address": {"street": "Main Street","city": "City","country": "USA"}}
{"id": 2,"name": "Jane","age": 30,"address": {"street": "Second Street","city": "City DC","country": "USA"}}
{"id": 3,"name": "Bob","age": 35,"address": {"street": "Third Street","city": "City District Columbia","country": "USA"}}
``` 
**file_2.json** :
```json
{"id": 1,"name": "John","age": 25,"address": {"street": "Main Street","city": "City","country": "USA"}}
{"id": 2,"name": "Jane","age": 30,"address": {"street": "Second Street","city": "City-DC","country": "USA"}}
{"id": 3,"name": "Bob","age": 35,"address": {"street": "Third Street","city": "City DistrictColumbia","country": "USA"}}
``` 

### JSON Request Path

<br>

To compare the values of the cities in this database with a <a href="#distance">distance</a> of 1: 

<br>
<br>

- **Distance:** 1

- **JSON Request Path:** $.address.city

<br>

The result obtain, in the **"key|JSON file "** format, will be:

<br>

- **Json_Request_Path_File_1.txt**:
```json
City|{"id": 1,"name": "John","age": 25,"address": {"street": "Main Street","city": "City","country": "USA"}}
City DC|{"id": 2,"name": "Jane","age": 30,"address": {"street": "Second Street","city": "City DC","country": "USA"}}
City District Columbia|{"id": 3,"name": "Bob","age": 35,"address": {"street": "Third Street","city": "City District Columbia","country": "USA"}}
```
- **Json_Request_Path_File_2.txt**:
```json
City|{"id": 1,"name": "John","age": 25,"address": {"street": "Main Street","city": "City","country": "USA"}}
City-DC|{"id": 2,"name": "Jane","age": 30,"address": {"street": "Second Street","city": "City-DC","country": "USA"}}
City DistrictColumbia|{"id": 3,"name": "Bob","age": 35,"address": {"street": "Third Street","city": "City DistrictColumbia","country": "USA"}}
```


### Fuzzy Join Algorithm

This result, allow now, to compare the keys of this key|JSON file format. using one of the algorithm of the project, the result will be:

```json
(City|{"id": 1,"name": "John","age": 25,"address": {"street": "Main Street","city": "City","country": "USA"}},City|{"id": 1,"name": "John","age": 25,"address": {"street": "Main Street","city": "City","country": "USA"}})
(City DC|{"id": 2,"name": "Jane","age": 30,"address": {"street": "Second Street","city": "City DC","country": "USA"}},City-DC|{"id": 2,"name": "Jane","age": 30,"address": {"street": "Second Street","city": "City-DC","country": "USA"}})
(City-DC|{"JSON file..."},City DC|{"JSON file..."})
(City District Columbia|{"JSON file..."},City DistrictColumbia|{"JSON file..."})
(City DistrictColumbia|{"JSON file..."},City District Columbia|{"JSON file..."})
```
## Prerequisites

<br>

- Java
- Spark
- [JSONPath](https://mvnrepository.com/artifact/com.nebhale.jsonpath/jsonpath/1.2) jar (*jsonpath-1.2.jar*)
- Maven (to modify or build the project)

<br>

## Usage

<br>

Examples of the use of this project are available in the [Running_File](https://gitlab.com/remiuh/fuzzy-join/-/blob/de7ef6509c0d68729f53e6aa2514ac8d00f47ca7/Running_Files/Run_Spark_Main.sh) file.

The project has two main ways to run from the command line:
- Main.java : run a search in a structured data tree and use a similarity search algorithm in one call. Call, for BH1 and Vobabulary, Vocabulary.java which create the <a href="#vocabulary">vocabulary</a> of the datasets.

    **Example of use** :
    ```bash
    /usr/local/spark/bin/spark-submit --class JSON.Main \ #Main class
    --jars ~/fuzzy-join/target/thesis-1.0-SNAPSHOT.jar, ~/jsonpath-1.2.jar \ #Jar files
    ~/file_1.json \  #dataset path 1
    ~/file_2.json \  #Dataset path 2
    $.address.city \ #JSON request path
    $.address.city \ #JSON request path
    ~/output/ \      #Output path
    null \           #Vocabulary path
    Naive \          #Fuzzy join Algorithm
    1 \              #Distance
    -1               #Vocabulary status unuse
    ```
- *ClassName*.java: run a search in a structured data tree, create a <a href="#vocabulary">vocabulary</a> or use a similarity search algorithm in multi-call.

    **Example of use** :
    ```bash
        /usr/local/spark/bin/spark-submit --class JSON.selfjoins.JsonReqPath \ #One file Json Request path class
        --jars ~/fuzzy-join/target/thesis-1.0-SNAPSHOT.jar,~/jsonpath-1.2.jar \ #Jar files
        ~/file_1.json \         #Dataset path 1
        ~/output_JsonReqPath/ \  #Output path
        $.address.city          #JSON request path
    ```
    ```bash
        user/usr/local/spark/bin/spark-submit --class JSON.selfjoins.JsonReqPath \ #One file Json Request path class
        --jars ~/fuzzy-join/target/thesis-1.0-SNAPSHOT.jar,~/jsonpath-1.2.jar \ #Jar files
        ~/file_2.json \         #Dataset path 2
        ~/output_JsonReqPath/ \  #Output path
        $.address.city          #JSON request path
    ```
    ```bash
        user/usr/local/spark/bin/spark-submit --class JSON.twoway.Naive \ #Two files Naive class
        --jars ~/fuzzy-join/target/thesis-1.0-SNAPSHOT.jar,~/jsonpath-1.2.jar \ #Jar files
        ~/file_1.json \ #Dataset path 1
        ~/file_2.json \ #Dataset path 2
        ~/output/ \     #Output path
        0               #Key position
        1               #Distance
    ```

## Support

<br>

For support :
- Rémi UHARTEGARAY : [remi.uh@gmail.com](mailto:remi.uh@gmail.com)  :  Project developer and engineering student at ENSSAT engineering school, France.
- Matthew DAMIGOS : [mgdamig@gmail.com](mailto:mgdamig@gmail.com) : Project supervisor and professor at the Ionnian University at Corfu, Greece.
- Shengyao XIAO : [shengyao.xiao0718@gmail.com](mailto:shengyao.xiao0718@gmail.com)  :  Project developer and engineering student at POLYTECH Nantes, France.

## Roadmap

<br>

- Add comparison of JSON file structures.
- Add comparison of JSON file nodes.
- Combine value and structure comparisons.
- Combine value, node and structure comparisons.

<br>

## Authors and acknowledgment

<br>

- Rémi UHARTEGARAY : [remi.uh@gmail.com](mailto:remi.uh@gmail.com)  :  Engineering student at ENSSAT engineering school, France.
- Matthew DAMIGOS : [mgdamig@gmail.com](mailto:mgdamig@gmail.com) : Professor at the Ionnian University at Corfu, Greece.
- Shengyao XIAO : [shengyao.xiao0718@gmail.com](mailto:shengyao.xiao0718@gmail.com)  :  Project developer and engineering student at POLYTECH Nantes, France.
- Laurent D'ORAZIO : [laurent.dorazio@univ-rennes1.fr](mailto:laurent.dorazio@univ-rennes1.fr) : Professor at the University of Rennes 1, France.

<br>

## Glossary
<br>

#### **Distance**
The Levenshtein distance, used here, between two strings is the minimum number of single-character edits (insertions, deletions or substitutions) required to change one string into the other.
<br>

#### **JSON request path**
<br>
The path to the value to compare in the JSON file.

<br>

#### **Key position**
<br>
The position of the key to compare in the dataset.

<br>

#### **Similarity**
The similarity between two strings is how much they are similar. It is calculated by the Levenshtein distance between two strings. Delete, insert and substitute operations are counted as 1.
<br>

#### **Voc status**

<br>

The status of the vocabulary. 
Possible values are:
- not used (-1)  : JsonReqPath, Naive, Splitting
- it is read (0) : Used in BH1 algorithm if a vocabulary already exists. 
- it is create (1): Used in BH1 algorithm if a vocabulary does not exist and need to be create.

<br>

#### **Vocabulary**
A list of all the values of a JSON request path in a dataset. That is used to reduce the number of comparisons between the values of the JSON request path in BH1 algorithm. Sample: ['C','i','t','y',...]

## Project status

The project was developed during a 3-month internship abroad at the Ionian University of Corfu, Greece. The project code runs on a computer simulating Spark parallelization and task distribution. It has been developed without using clusters, so it is possible to experience problems when running on them.