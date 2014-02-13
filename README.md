
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

    for filename in *.splits; do
      tablename=$(echo ${filename} | sed -e 's/example_//' -e 's/\.splits//')
      /usr/lib/accumulo/bin/accumulo shell -u root -p password -e "addsplits -t ${tablename} -sf ${filename}"
    done

1. Submit the MR job:

    hadoop jar lumify-wikipedia-mr-*-jar-with-dependencies.jar /lumify/enwiki-20140102-pages-articles.MR.txt

1. Wait for the MR job to complete

1. Run the open source Storm topology to perform text highlighting (and index the data in Elastic Search)
