#!/usr/bin/env bash

N=$1
ORIG=$2

for i in {1..300}
do
  A=''
  size=${#i}
  if [ $size == 1 ]
   then
     A="00"
  fi

  if [ $size == 2 ]
  then
      A="0"
  fi

 hadoop-common/bin/hadoop dfs -copyFromLocal hadoop_in/$ORIG/large$A$i''_0_.txt /user/hadoop_dfs/hadoop_in/$ORIG/large$A$i''_0_.txt
done

