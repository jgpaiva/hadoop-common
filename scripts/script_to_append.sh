#!/usr/bin/env bash

for i in {2..320..2}
do
	A=large
	B=_0_.txt
		
	size=${#i}
	if [ "$size" != 3 ]
	 then
		A=large0
	fi
 
        size=${#i} 
	if [ "$size" == 1 ]
	 then
		C=0
		A=$A$C
	fi

	FILE=$A$i$B

	COMMAND="echo \"manel\" >> large_10/"
	COMMAND=$COMMAND$FILE

        echo "$COMMAND"
done
