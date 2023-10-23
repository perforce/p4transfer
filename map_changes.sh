#!/bin/bash
# Grab a load of P4Transfer'ed changes and extra target/source mappings

# p4 changes -ltm999 //UE5/Release-Engine-Staging/... | 


show_help_info () {
echo -e "\n\tERROR: $1"

cat <<HELPINFO
---
Usage:

    ./map_changes.sh <num_changes> <path>

Example:

    ./map_changes.sh 99 //UE5/Release-5.0/...

HELPINFO
}

function msg () { echo -e "$*"; }
function bail () { msg "\nError: ${1:-Unknown Error}\n"; exit ${2:-1}; }

# -------------------------------------------------------------------------
if [ -z "$1" ];then
    show_help_info "No <num_changes> parameter specified"
    exit 1
fi
if [ -z "$2" ];then
    show_help_info "No <depot_path> parameter specified"
    exit 1
fi

# Get path/file parm
max_changes=$1
depot_path=$2

src=0
targ=0

p4 changes -lm $max_changes $depot_path | egrep "^Change |^\s+Transferred from p4:" | while read -e line
do
   
    if [[ $line =~ ^Change\ ([0-9]+) ]]; then
        targ="${BASH_REMATCH[1]}"
    fi
    if [[ $line =~ Transferred\ from.*\@([0-9]+)$ ]]; then
        src="${BASH_REMATCH[1]}"
        echo "$src,$targ"
    fi
done
