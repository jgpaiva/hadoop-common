#!/bin/bash

#./copy_to_dfs.sh 300 large_random


for split in 25 50
do
  for ratio in 50
  do
    for repetition in {1..5}
    do
      hadoop-common/bin/hadoop jar hadoop-common/hadoop*examples*.jar selection selection_ci_$split''_rat$ratio''_$repetition'' $split 0-relevant_$ratio''_$repetition'' 0 /user/hadoop_dfs/hadoop_in/large_random/ /user/hadoop_dfs/results_ci_$split''_rat$ratio''_$repetition
    done	
  done
done

#./copy_to_dfs.sh 300 large_1
#./copy_to_dfs.sh 300 large_10
#./copy_to_dfs.sh 300 large_20


#WITH INDEXES
#hadoop-common/bin/hadoop jar hadoop-common/hadoop*examples*.jar selection selection_ci_1_1 1 0-manel 0 /user/hadoop_dfs/hadoop_in/large_1/ /user/hadoop_dfs/results_ci_1_1
#hadoop-common/bin/hadoop jar hadoop-common/hadoop*examples*.jar selection selection_ci_1_10 1 0-manel 0 /user/hadoop_dfs/hadoop_in/large_10/ /user/hadoop_dfs/results_ci_1_10
#hadoop-common/bin/hadoop jar hadoop-common/hadoop*examples*.jar selection selection_ci_1_20 1 0-manel 0 /user/hadoop_dfs/hadoop_in/large_20/ /user/hadoop_dfs/results_ci_1_20

#hadoop-common/bin/hadoop jar hadoop-common/hadoop*examples*.jar selection selection_ci_10_1 10 0-manel 0 /user/hadoop_dfs/hadoop_in/large_1/ /user/hadoop_dfs/results_ci_10_1
#hadoop-common/bin/hadoop jar hadoop-common/hadoop*examples*.jar selection selection_ci_10_10 10 0-manel 0 /user/hadoop_dfs/hadoop_in/large_10/ /user/hadoop_dfs/results_ci_10_10
#hadoop-common/bin/hadoop jar hadoop-common/hadoop*examples*.jar selection selection_ci_10_20 10 0-manel 0 /user/hadoop_dfs/hadoop_in/large_20/ /user/hadoop_dfs/results_ci_10_20

#hadoop-common/bin/hadoop jar hadoop-common/hadoop*examples*.jar selection selection_ci_25_1 25 0-manel 0 /user/hadoop_dfs/hadoop_in/large_1/ /user/hadoop_dfs/results_ci_25_1
#hadoop-common/bin/hadoop jar hadoop-common/hadoop*examples*.jar selection selection_ci_25_10 25 0-manel 0 /user/hadoop_dfs/hadoop_in/large_10/ /user/hadoop_dfs/results_ci_25_10
#hadoop-common/bin/hadoop jar hadoop-common/hadoop*examples*.jar selection selection_ci_25_20 25 0-manel 0 /user/hadoop_dfs/hadoop_in/large_20/ /user/hadoop_dfs/results_ci_25_20

#hadoop-common/bin/hadoop jar hadoop-common/hadoop*examples*.jar selection selection_ci_50_1 50 0-manel 0 /user/hadoop_dfs/hadoop_in/large_1/ /user/hadoop_dfs/results_ci_50_1
#hadoop-common/bin/hadoop jar hadoop-common/hadoop*examples*.jar selection selection_ci_50_10 50 0-manel 0 /user/hadoop_dfs/hadoop_in/large_10/ /user/hadoop_dfs/results_ci_50_10
#hadoop-common/bin/hadoop jar hadoop-common/hadoop*examples*.jar selection selection_ci_50_20 50 0-manel 0 /user/hadoop_dfs/hadoop_in/large_20/ /user/hadoop_dfs/results_ci_50_20

#hadoop-common/bin/hadoop jar hadoop-common/hadoop*examples*.jar selection selection_ci_100_1 100 0-manel 0 /user/hadoop_dfs/hadoop_in/large_1/ /user/hadoop_dfs/results_ci_100_1
#hadoop-common/bin/hadoop jar hadoop-common/hadoop*examples*.jar selection selection_ci_100_10 100 0-manel 0 /user/hadoop_dfs/hadoop_in/large_10/ /user/hadoop_dfs/results_ci_100_10
#hadoop-common/bin/hadoop jar hadoop-common/hadoop*examples*.jar selection selection_ci_100_20 100 0-manel 0 /user/hadoop_dfs/hadoop_in/large_20/ /user/hadoop_dfs/results_ci_100_20

#WITHOUT INDEX
#hadoop-common/bin/hadoop jar hadoop-common/hadoop*examples*.jar selection selection_ni_1_1 1 none 0 /user/hadoop_dfs/hadoop_in/large_1/ /user/hadoop_dfs/results_ni_1_1
#hadoop-common/bin/hadoop jar hadoop-common/hadoop*examples*.jar selection selection_ni_1_10 1 none 0 /user/hadoop_dfs/hadoop_in/large_10/ /user/hadoop_dfs/results_ni_1_10
#hadoop-common/bin/hadoop jar hadoop-common/hadoop*examples*.jar selection selection_ni_1_20 1 none 0 /user/hadoop_dfs/hadoop_in/large_20/ /user/hadoop_dfs/results_ni_1_20

#hadoop-common/bin/hadoop jar hadoop-common/hadoop*examples*.jar selection selection_ni_10_1 10 none 0 /user/hadoop_dfs/hadoop_in/large_1/ /user/hadoop_dfs/results_ni_10_1
#hadoop-common/bin/hadoop jar hadoop-common/hadoop*examples*.jar selection selection_ni_10_10 10 none 0 /user/hadoop_dfs/hadoop_in/large_10/ /user/hadoop_dfs/results_ni_10_10
#hadoop-common/bin/hadoop jar hadoop-common/hadoop*examples*.jar selection selection_ni_10_20 10 none 0 /user/hadoop_dfs/hadoop_in/large_20/ /user/hadoop_dfs/results_ni_10_20

#hadoop-common/bin/hadoop jar hadoop-common/hadoop*examples*.jar selection selection_ni_25_1 25 none 0 /user/hadoop_dfs/hadoop_in/large_1/ /user/hadoop_dfs/results_ni_25_1
#hadoop-common/bin/hadoop jar hadoop-common/hadoop*examples*.jar selection selection_ni_25_10 25 none 0 /user/hadoop_dfs/hadoop_in/large_10/ /user/hadoop_dfs/results_ni_25_10
#hadoop-common/bin/hadoop jar hadoop-common/hadoop*examples*.jar selection selection_ni_25_20 25 none 0 /user/hadoop_dfs/hadoop_in/large_20/ /user/hadoop_dfs/results_ni_25_20

#hadoop-common/bin/hadoop jar hadoop-common/hadoop*examples*.jar selection selection_ni_50_1 50 none 0 /user/hadoop_dfs/hadoop_in/large_1/ /user/hadoop_dfs/results_ni_50_1
#hadoop-common/bin/hadoop jar hadoop-common/hadoop*examples*.jar selection selection_ni_50_10 50 none 0 /user/hadoop_dfs/hadoop_in/large_10/ /user/hadoop_dfs/results_ni_50_10
#hadoop-common/bin/hadoop jar hadoop-common/hadoop*examples*.jar selection selection_ni_50_20 50 none 0 /user/hadoop_dfs/hadoop_in/large_20/ /user/hadoop_dfs/results_ni_50_20

#hadoop-common/bin/hadoop jar hadoop-common/hadoop*examples*.jar selection selection_ni_100_1 100 none 0 /user/hadoop_dfs/hadoop_in/large_1/ /user/hadoop_dfs/results_ni_100_1
#hadoop-common/bin/hadoop jar hadoop-common/hadoop*examples*.jar selection selection_ni_100_10 100 none 0 /user/hadoop_dfs/hadoop_in/large_10/ /user/hadoop_dfs/results_ni_100_10
#hadoop-common/bin/hadoop jar hadoop-common/hadoop*examples*.jar selection selection_ni_100_20 100 none 0 /user/hadoop_dfs/hadoop_in/large_20/ /user/hadoop_dfs/results_ni_100_20

