pid=`ps -ef|grep 'riskwarning.msg-0.0.1.jar'|grep -v grep|awk '{print $2}'`; 
echo "start to kill process:$pid"
kill $pid
echo "kill end."