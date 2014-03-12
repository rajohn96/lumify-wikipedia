## Wikipedia Import via Map Reduce

1. Build the jar:

    mvn clean package -P uber-jar

1. Convert the well-formed XML to one XML page element per line:

        java -cp umify-wikipedia-mr-*-jar-with-dependencies.jar \
          com.altamiracorp.lumify.wikipedia.mapreduce.WikipediaFileToMRFile \
          -in enwiki-20140102-pages-articles.xml
          -out enwiki-20140102-pages-articles.MR.txt

1. Copy the MR input file to HDFS:

        hadoop fs -mkdir -p /lumify
        hadoop fs -put enwiki-20140102-pages-articles.MR.txt /lumify

1. Pre-split destination Accumulo tables:

        bin/configure_splits.sh

1. Submit the MR job:

        hadoop jar lumify-wikipedia-mr-*-jar-with-dependencies.jar /lumify/enwiki-20140102-pages-articles.MR.txt

1. Wait for the MR job to complete

1. Run the open source Storm topology to perform text highlighting (and index the data in Elastic Search)

## Running inside an IDE

1. Run format

1. Import the dev.owl

1. Import the wikipedia.owl

1. Run `com.altamiracorp.lumify.wikipedia.mapreduce.ImportMR enwiki-20140102-pages-articles-lines-10.xml`

1. Run the public storm topology.
