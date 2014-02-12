
To Import via map reduce job:

1. Run `mvn clean compile package -Puber-jar`
1. Copy `lumify-wikipedia-mr/target/lumify-wikipedia-mr-*-jar-with-dependencies.jar` to your hadoop cluster.
1. Create splits in atc_securegraph_d, atc_securegraph_e, atc_securegraph_v, atc_termMention tables.
1. Run `sudo -u hdfs hadoop jar lumify-wikipedia-mr-*-jar-with-dependencies.jar enwiki-20140102-pages-articles-lines.xml`
