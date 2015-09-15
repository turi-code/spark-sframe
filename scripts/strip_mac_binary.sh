#!/usr/bin/env bash


f=src/main/resources/org/graphlab/create/spark_unity_mac

strip -S $f

install_name_tool -change libpython2.7.dylib @rpath/libpython2.7.dylib $f || true
install_name_tool -change /System/Library/Frameworks/Python.framework/Versions/2.7/Python @rpath/libpython2.7.dylib $f || true

relative=`echo $f | sed 's:\./::g' | sed 's:[^/]*/:../:g' | sed 's:[^/]*$::'`
# Now we need ../../ to get to PREFIX/lib
rpath="@loader_path/../../$relative"
install_name_tool -add_rpath $rpath $f || true
