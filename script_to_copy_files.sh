#!/usr/bin/env bash

for i in {3..60}
do
	A=large
	B=_0_.txt
	FILE=$A$i$B

	COMMAND='cp large/large2_0_.txt large/'
	COMMAND=$COMMAND$FILE

	$COMMAND
done
