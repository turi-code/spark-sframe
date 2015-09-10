#!/usr/bin/env bash

path=src/main/resources/org/graphlab/create/spark_unity_mac

strip -S $path

install_name_tool -change @rpath/libpython2.7.dylib /Library/Frameworks/Python.framework/Versions/2.7/lib/libpython2.7.dylib $path
