#!/bin/bash

function _write_split_file {
  local prefix=$1
  local filename=$(mktemp -t $(basename $0).XXX)

  for a in A B C D E F G H I J K L M N O P Q R S T U V W X Y Z; do
    echo "${prefix}${a}" >> ${filename}
  done

  echo ${filename}
}

function _configure_splits {
  local tablename=$1
  local prefix=$2

  local filename=$(_write_split_file ${prefix})
  /usr/lib/accumulo/bin/accumulo shell -u root -p password -e "addsplits -t ${tablename} -sf ${filename}"
  rm -f ${filename}
}


_configure_splits atc_securegraph_d DVWIKIPEDIA_
_configure_splits atc_securegraph_e EWIKIPEDIA_LINK_
_configure_splits atc_securegraph_v VWIKIPEDIA_
_configure_splits atc_termMention   WIKIPEDIA_
