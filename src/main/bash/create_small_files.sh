#!/usr/bin/env bash
#MY_MESSAGE="Hello World"
#echo $(( ( RANDOM % 10 )  + 1 ))

filename="small"
for i in {1000..1350}
do
	filename=$filename$i
	#echo $filename
	#delay_time=$(((RANDOM%5)+1))
	count_of_dump=$(((RANDOM%2)+7))
	#sleep $delay_time
    dd if=/dev/zero of=./small-15G-2/$filename bs=10M count=$count_of_dump
    filename="small"
done
