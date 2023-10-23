#!/bin/bash
# Runs compares against all depot paths found in target client.
# Assumptions:
#   - target dir exists and P4CONFIG is setup
#   - source/target use same depot paths

configFile=ue_config.yaml
compare=../p4transfer.git/CompareRepos.py
counterName=$(grep ^counter_name: $configFile | awk '{print $2}')
export P4CONFIG=.p4config.txt

pushd target
readarray -t lines < <(p4 client -o | grep -A99 ^View: | grep -v View: | awk '{print $1}')
counter=$(p4 counters | grep "$counterName" | awk '{print $3}')

popd

for v in "${lines[@]}"
do
    output="${v//\//_}"
    output="${output//./}"
    $compare -c $configFile -s "$v@$counter" > "compare${output}.out"
    differences=$(grep "Sum Total:" "compare${output}.out" | tail -1 | awk '{print $3}')
    if [[ $differences -gt 0 ]]; then
        $compare -c $configFile -s "$v@$counter" --fix
        pushd target
        p4 --field Description="Fix differences from snapshot of $v@$counter" change -o | p4 change -i
        popd
    fi
done
