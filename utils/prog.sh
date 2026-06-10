#!/bin/bash
# Summarise progress for a long running P4Transfer.py
# Intended to be called by companion script report_prog.sh in same dir (run from a crontab)
#
# Can be run with "-n" as a parameter, otherwise it updates current progress.

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

# Config section - adjust if required
transfer_dir="$SCRIPT_DIR"
mkdir -p $transfer_dir/logs
mkdir -p $transfer_dir/source
# End of config

# Specify head change instead of tail
change_specified=${1:-Unset}
no_update=0
if [[ "$1" = "-n" ]]; then
    no_update=1
    change_specified=Unset
fi

dt=$(date +"%Y/%m/%d %H:%M:%S")
echo "$dt"
dtsuffix="$dt"

sizes_file=$transfer_dir/source/sizes.txt
last_report=$transfer_dir/prog.out
old_report=$transfer_dir/logs/prog.out.$dtsuffix

export lastlogfile=$(ls -tr $transfer_dir/log*.log | tail -1)
last_change=$(grep :INFO: $lastlogfile | grep 'source =' | tail -1 | awk '{print $5}')
[[ "$change_specified" != "Unset" ]] && last_change=$(grep :INFO: $lastlogfile | grep 'source =' | head -1 | awk '{print $5}')
if [[ -z "$last_change" ]]; then
    echo "No progress in current log file"
    exit 0
fi

grep -n2 $last_change $transfer_dir/source/sizes.txt
line_no=$(grep -n "=$last_change " $sizes_file | cut -d: -f1)
lines=$(cat $sizes_file | wc -l)
if [[ -z "$line_no" ]]; then
    echo "Need to update $sizes_file"
    line_no=$lines
fi
cpercent=$(awk "BEGIN{ print ($line_no / $lines) * 100}")

# Calculate total sizes to process - pretty fast even for large files
awkcmd='
{f += $2; l = length($4); s = substr($4, 1, l-1);
u = substr($4, l, 1); a[u] += s
} END {
#    print "files " f;
#    print "sizes ";
#    for (k in a) print k, a[k]
    print f, a["G"] + a["M"]/1024
}
'

totals=$(cat $sizes_file | awk "$awkcmd")
progressTotals=$(head -$line_no $sizes_file | awk "$awkcmd")
atotals=($totals)
ptotals=($progressTotals)

# echo "$totals $progressTotals"
fpercent=$(awk "BEGIN{ print (${ptotals[0]} / ${atotals[0]}) * 100}")
spercent=$(awk "BEGIN{ print (${ptotals[1]} / ${atotals[1]}) * 100}")

# Report progress
echo ""
echo "$dt: Totals: Changes $line_no/$lines, Files ${ptotals[0]}/${atotals[0]}, Sizes ${ptotals[1]}/${atotals[1]}"
echo "                Percentages: Changes $cpercent%, Files $fpercent%, Sizes $spercent%"
echo ""
echo "Previous run"
grep Totals $last_report
num_processed=$(grep Totals $last_report | perl -pe 's/.* Changes (\d+).*/$1/')
echo "Changes processed since then: $(($line_no - $num_processed))"

# Save last report file
[[ -f $last_report && $no_update -eq 0 ]] && mv $last_report $old_report
if [[ $no_update -eq 0 ]]; then
    echo "$dt: Totals: Changes $line_no/$lines, Files ${ptotals[0]}/${atotals[0]}, Sizes ${ptotals[1]}/${atotals[1]}" > $last_report
    echo "           Percentages: Changes $cpercent%, Files $fpercent%, Sizes $spercent%" >> $last_report
    echo "Changes processed: $(($line_no - $num_processed))"  >> $last_report
fi

echo ""
echo "Tail of :INFO lines in log:"
echo "=========================="
grep :INFO: $lastlogfile | tail
echo ""
echo "Tail of log in case of errors:"
echo "============================="
tail $lastlogfile | cut -c -300
