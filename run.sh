#!/usr/bin/env bash
if [ -d "search_results" ]; then
  # Control will enter here if $DIRECTORY exists.
  rm -rf search_results
fi

java -jar reuter-searcher.jar $1 $2
