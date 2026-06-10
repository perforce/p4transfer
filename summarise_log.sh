#!/bin/bash
# Produce a summary log file for reporting problems with P4Transfer.
#

usage() {
    cat <<'EOF'
Usage: summarise_log.sh [LOG_FILE] > sum.log

Summarise a P4Transfer log failure by printing an initial section and the details of the final failing
changelist.

Please run this script on your log file and send the results to consulting@perforce.com or your
direct Perforce contact.

Arguments:
  LOG_FILE   Log file to summarise.

If LOG_FILE is omitted, the script uses the current value of $lastlogfile.
EOF
}

if [[ "$1" == "-h" || "$1" == "--help" ]]; then
	usage
	exit 0
fi

log_file=${1:-$lastlogfile}

if [[ -z "$log_file" ]]; then
	echo "Error: no log file provided and \$lastlogfile is empty." >&2
	usage >&2
	exit 1
fi

if [[ ! -f "$log_file" ]]; then
	echo "Error: log file not found: $log_file" >&2
	exit 1
fi

head_count=$(head -400 "$log_file" | grep -n "DEBUG: reading changes: " | head -1 | cut -d: -f1)

last_change_line_no=$(grep -n "Processing change:" "$log_file" | tail -1 | cut -d: -f1)
lines=$(wc -l "$log_file" | sed -E 's/^ *//' | cut -d" " -f1)
tail_count=$(( lines - last_change_line_no ))

head -"$head_count" "$log_file"
echo "==============================="
tail -"$tail_count" "$log_file"
