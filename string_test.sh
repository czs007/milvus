#!/usr/bin/env bash


#if [[ "$OSTYPE" == "linux-gnu"* ]]; then
#        LIBJEMALLOC=$PWD/internal/core/output/lib/libjemalloc.so
#        if test -f "$LIBJEMALLOC"; then
#                #echo "Found $LIBJEMALLOC"
#                export LD_PRELOAD="$LIBJEMALLOC"
#        else
#                echo "WARN: Cannot find $LIBJEMALLOC"
#        fi
#fi


STRLEN=129
datatype=string

#ulimit -c unlimited
#GOTRACEBACK=crash ./bin/scalar -load_num 1 -type string -str_len ${STRLEN} -num 40000000  >> string_result.txt
#./bin/scalar -load_num 1 -type string -str_len ${STRLEN} -num 40000000  >> string_result.txt
#./bin/scalar -load_num 1 -type string -str_len ${STRLEN} -num 80000000  >> string_result.txt
#exit 0
#./bin/scalar -load_num 1 -type string -str_len ${STRLEN} -num 50000000  >> string_result.txt
./bin/scalar -load_num 1 -type ${datatype} -str_len ${STRLEN} -num 200000  >> string_result.txt
./bin/scalar -load_num 1 -type ${datatype} -str_len ${STRLEN} -num 400000  >> string_result.txt
./bin/scalar -load_num 1 -type ${datatype} -str_len ${STRLEN} -num 800000  >> string_result.txt
./bin/scalar -load_num 1 -type ${datatype} -str_len ${STRLEN} -num 1000000  >> string_result.txt
./bin/scalar -load_num 1 -type ${datatype} -str_len ${STRLEN} -num 2000000  >> string_result.txt
./bin/scalar -load_num 1 -type ${datatype} -str_len ${STRLEN} -num 4000000  >> string_result.txt
./bin/scalar -load_num 1 -type ${datatype} -str_len ${STRLEN} -num 8000000  >> string_result.txt
./bin/scalar -load_num 1 -type ${datatype} -str_len ${STRLEN} -num 10000000  >> string_result.txt
./bin/scalar -load_num 1 -type ${datatype} -str_len ${STRLEN} -num 20000000  >> string_result.txt
