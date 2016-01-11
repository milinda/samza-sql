#!/usr/bin/env bash

[[ $JAVA_OPTS != *-Dlog4j.configuration* ]] && export JAVA_OPTS="$JAVA_OPTS -Dlog4j.configuration=file:$(dirname $0)/log4j-console.xml"

exec $(dirname $0)/run-class.sh org.apache.samza.sql.bench.job.BenchJobRunner "$@"