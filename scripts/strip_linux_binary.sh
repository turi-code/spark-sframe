#!/usr/bin/env bash

path=src/main/resources/org/graphlab/create/spark_unity_linux

# Remove dynamic link resolving paths
# yum install chrpath
chrpath -d $path

# strip symbols to reduce filesize
strip -S $path

