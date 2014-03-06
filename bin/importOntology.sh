#!/bin/bash

SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do
  DIR="$(cd -P "$(dirname "$SOURCE")" && pwd)"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE"
done
DIR="$(cd -P "$(dirname "$SOURCE")" && pwd)"

classpath=$(${DIR}/classpath.sh lumify-wikipedia-core)
if [ $? -ne 0 ]; then
  echo "${classpath}"
  exit
fi

java \
-Dfile.encoding=UTF-8 \
-classpath ${classpath} \
-Xmx1024M \
com.altamiracorp.lumify.core.cmdline.OwlImport \
--in=${DIR}/../data/ontology/wikipedia.owl
