#!/usr/bin/env bash

# $1    controller address
# $2    client base port
# $3    buffer size

trap "exit" INT
while true; do
    go run Client/Client.go -controller $1 -base_port $2 -buffer $3
    sleep 1
done
