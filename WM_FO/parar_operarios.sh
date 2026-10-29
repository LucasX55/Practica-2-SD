#!/bin/bash

docker rm -f $(docker ps -a -q --filter "name=fo-op") 2>/dev/null
echo "Operarios parados"