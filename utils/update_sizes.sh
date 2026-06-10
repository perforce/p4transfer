#!/bin/bash
# Update the sizes.txt file using transfer client
# Add to crontab:
#   */30 * * * *  cd /some_dir/p4transfer/source && ./update_sizes.sh >> upd.out 2>&1
# Parses and produces lines like this:
# @=18170780 1 files 12.2K bytes

workspace="My_p4transfer"
SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

pushd $SCRIPT_DIR > /dev/null
export P4CONFIG=.p4config.txt

printf -v date '%(%Y-%m-%d %H:%M:%S)T' -1
echo "$date: Starting to update sizes"

last_change=$(tail -1 sizes.txt | cut -d' ' -f 1| cut -d= -f2)

start_change=$(($last_change + 1))
p4 changes -t //$workspace/...@$start_change,now > _changes.txt
changes=$(cat _changes.txt | wc -l)
cp sizes.txt _sizes.txt
cat _changes.txt | cut -d' ' -f 2 | while read c
do
    p4 sizes -sh @=$c >> _sizes.txt
done
sort _sizes.txt > sizes.txt
printf -v date '%(%Y-%m-%d %H:%M:%S)T' -1
echo "$date: Finished updating sizes - new changes found: $changes"

popd > /dev/null

