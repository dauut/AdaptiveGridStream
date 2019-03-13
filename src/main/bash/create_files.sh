#!/usr/bin/env bash
#MY_MESSAGE="Hello World"
#echo $(( ( RANDOM % 10 )  + 1 ))

#credentials
#uid=$(id -u)
#cert_path="/tmp/x509up_u"
#proxy=$cert_path$uid

filename="file"
for i in {0..3}
do
	filename=$filename$i
	#echo $filename
	delay_time=$(((RANDOM%5)+1))
	count_of_dump=$(((RANDOM%5)+1))
	sleep $delay_time
    dd if=/dev/zero of=/tmp/davut/$filename bs=10M count=$count_of_dump
    filename="file"
done



