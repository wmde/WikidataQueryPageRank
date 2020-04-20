#!/bin/bash
GOLD_FILE=$1
echo "GOLDFILE = $GOLD_FILE"
if [ -z "$GOLD_FILE" ] ; then echo "usage: $(basename $0) <gold_ranks.csv>" ; exit 1 ; fi

CSVJSON="/usr/local/bin/csvjson"
FILENAMES=(0 \
  diseases lakes epidemic-movies people_with_covid-19 languages academy_awards wuhan-destinations german-directors \
)

header=$(head -n1 $GOLD_FILE)

ranking_threshold=10
for i in $(seq 1 8) ; do
  json_file="0${i}-${FILENAMES[$i]}.json"
  offset=$((1+$i*10))
  echo "creating $json_file by slicing $ranking_threshold items from $GOLD_FILE, starting at line $(($offset-10))"
  echo $header > tmp.csv
  head -n $offset $GOLD_FILE | tail -n 10 | grep -v "^\"\";" >> tmp.csv
  $CSVJSON -k qid tmp.csv > $json_file
  rm tmp.csv
done

