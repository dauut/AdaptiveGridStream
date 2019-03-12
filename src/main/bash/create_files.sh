#MY_MESSAGE="Hello World"
#echo $(( ( RANDOM % 10 )  + 1 ))

filename="file"
ten=10
for i in {000..100}
do
	filename=$filename$i
	#echo $filename
	delay_time=$(((RANDOM%5)+1))
	sleep $delay_time
    dd if=/dev/zero of=./files/$filename bs=1M count=3
    filename="file"
done

