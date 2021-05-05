#!/bin/bash

time="1"     # one second
int="eth0"   # network interface eth0

log_file="network.log"

while true
	do
		txpkts_old="`cat /sys/class/net/$int/statistics/tx_packets`" # sent packets
		rxpkts_old="`cat /sys/class/net/$int/statistics/rx_packets`" # recv packets
		txbytes_old="`cat /sys/class/net/$int/statistics/tx_bytes`" # sent bytes
    		rxbytes_old="`cat /sys/class/net/$int/statistics/rx_bytes`" # recv bytes
			sleep $time
		txpkts_new="`cat /sys/class/net/$int/statistics/tx_packets`" # sent packets
    		rxpkts_new="`cat /sys/class/net/$int/statistics/rx_packets`" # recv packets
    		txbytes_new="`cat /sys/class/net/$int/statistics/tx_bytes`" # sent bytes
    		rxbytes_new="`cat /sys/class/net/$int/statistics/rx_bytes`" # recv bytes

		txpkts="`expr $txpkts_new - $txpkts_old`"	# evaluate expressions for sent packets
		rxpkts="`expr $rxpkts_new - $rxpkts_old`"	# evaluate expressions for recv packets

    		txbytes="`expr $txbytes_new - $txbytes_old`" # evaluate expressions for sent bytes
    		rxbytes="`expr $rxbytes_new - $rxbytes_old`" # evaluate expressions for recv bytes
    		tpbyte="`expr $txbytes + $rxbytes`" # evaluate expressions for throughput in bytes

		echo "tx $txpkts pkts/s - rx $rxpkts pkts/s - tp $tpbyte bytes/s on interface $int"
    		echo "tx $txpkts pkts/s - rx $rxpkts pkts/s - tp $tpbyte bytes/s on interface $int" >> $log_file
	done
