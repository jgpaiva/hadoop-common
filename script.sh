#!/usr/bin/env bash

# 
array=(1 2 4 8 12 16 18 22 26 30)
J=0

for i in "${array[@]}"
do
	FILES1='bin/hadoop dfs -copyFromLocal ../hadoop_in/large/large'
	FILES2='_1_.txt /user/hadoop_dfs/hadoop_in/large'
	FILES3='_1_.txt'

	echo $J

	if [[ "$J" != 0 ]]; then
		START=$((${array[$J-1]}+1))
               	for k in $(eval echo "{$START..$i}")
		do
			FILES=$FILES1$k$FILES2$k$FILES3
			$FILES
		done
	else        
		FILES=$FILES1$i$FILES2$i$FILES3
		$FILES
	fi

	EXE1='bin/hadoop jar hadoop*examples*.jar wordcountmy /user/hadoop_dfs/hadoop_in/ /user/hadoop_dfs/results'
	EXE=$EXE1$i
	$EXE


        sleep 5


	J=$((J+1))
done
