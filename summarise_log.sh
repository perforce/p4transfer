#!/bin/bash
# Produce a summary log file for reporting problems with P4Transfer.
# Please run this script on your log file and send the results to consulting@perforce.com or your
# direct Perforce contact.
#
# It extracts the header information and the details of the failing changelist at the end of the log
# file.
#
# Usage:
#   sum_log.sh log-P4Transfer-20141208094716.log > sum.log

log_file=$1

head_count=`head -200 $log_file | grep -n "src('changes'" | head -1 | cut -d: -f1`

last_change_line_no=`grep -n "Processing change:" $log_file | tail -1 | cut -d: -f1`
lines=`wc -l $log_file | sed -E 's/^ *//' | cut -d" " -f1`
tail_count=$(( $lines - $last_change_line_no ))

head -$head_count $log_file
tail -$tail_count $log_file
