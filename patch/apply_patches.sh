#!/bin/bash

DIRNAME=$0
if [ "${DIRNAME:0:1}" = "/" ];then
    CURDIR=`dirname $DIRNAME`
else
    CURDIR="`pwd`"/"`dirname $DIRNAME`"
fi
SUBMODULE_SEASTAR_PATH=$CURDIR"/../seastar"
cd $SUBMODULE_SEASTAR_PATH

patch_files=$(ls ../patch/*.patch)

for patch_file in $patch_files
do
    echo "Applying patch: $patch_file"
    git apply --whitespace=fix $patch_file
    if [ $? -eq 0 ]; then
        echo "Patch applied successfully!"
    else
        echo "Failed to apply patch: $patch_file"
        exit 1
    fi
done

echo "All patches applied successfully!"
