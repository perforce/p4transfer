# p4transfer
Utility for transferring a subset of files (with complete history of all changelists) from one Helix Core repository to another, only requiring read access to the source repository.

# README for P4Transfer.py

The script `P4Transfer.py` is for use when transferring changes between 2 Perforce Servers (one way).

See [P4Transfer.docx](P4Transfer.docx) for more details as to what it does and how to use it.

It does not handle bi-directional transfers, and it expects the target server *path* to not be modified by anything other than P4Transfer.

## P4Transfer and Helix native DVCS

Both Helix native DVCS and P4Transfer enable migration of detailed file history from a set of paths on one Helix Core server into another server.
 
Helix native DVCS has additional functionality, such as creating a personal micro-repo for personal use without a continuously running ‘p4d’ process.

Helix native DVCS vs. P4Transfer for file migration:
* Helix native DVCS is a fully supported product feature.
* Helix native DVCS requires consistency among the Helix Core servers: Same case sensitivity setting, same Unicode mode, and same P4D version (with some exceptions).
* Helix native DVCS is easy to setup and use.
* Helix native DVCS has some limitations at large scale (depends on server RAM).
* Helix native DVCS is generally deemed to product the highest possible quality data on the target server, as it transfers raw journal data and archive content.
* Helix native DVCS is very fast.
* If Helix native DVCS hits data snags, there may not be an easy way to work around them, unless a new P4D server release address them.
* Helix native DVCS must be explicitly enabled.  If not already enabled, it requires ‘super’ access on both Helix Core servers involved in the process.
* P4Transfer is community supported (and with paid Consulting engagements).
* P4Transfer can make timestamps more accurate if it has ‘admin’ level access, but does not require it.
* P4Transfer automates front-door workflows, i.e. it does would a human with a ‘p4’ command line client, fleet fingers and a lot of time could do.
* P4Transfer can work with mismatched server data sets (different case sensitivity, Unicode mode, or P4D settings) *so long as* the actual data to be migrated doesn’t cause issues.  (For example, if you try to copy paths that actually do have Korean glyphs in the path name to a non-Unicode server, that ain’t gonna work.).
* If P4Transfer does have issues with importing some data (like the above example with Unicode paths), you can manually work around those snags, and then pick up with the next changelist.  If there aren’t too many snags, this trial and error process is viable.
* P4Transfer is reasonably fast as a front-door mechanism.
* P4Transfer can be run as a service for continuous operation, and has successfully run for months or more.
* P4Transfer data quality is excellent in terms of detail (e.g. integration history, resolve options, etc.).  However, it is an emulation of history rather than a raw replay of  actually history in the native format as done by Helix native DVCS.  The nuance in history differences rarely has practical implications.
* P4Transfer requires more initial setup than Helix native DVCS.
* P4Transer requires Python (2.7 or 3.6+).
* Helix native DVCS has no external dependencies.
* P4Transfer can be interfered with by custom policy enforcement triggers on the target server.  
* Helix native DVCS bypasses triggers.

Short: When it is an option, using Helix native DVCS is preferred.  But if that can’t be used for some reason, P4Transfer can pretty much always be made to work.

# Support

Any errors in the script are highly likely to be due to some unusual integration history, which may have been 
done with an older version of the Perforce server.

If you have an error when running the script, please use summarise_log.sh to create
a summary log file to send. E.g.

    summarise_log.sh log-P4Transfer-20141208094716.log > sum.log

If you get an error message in the log file such as:

    P4TLogicException: Replication failure: missing elements in target changelist: /work/p4transfer/main/applications/util/Utils.java
    
or

    P4TLogicException: Replication failure: src/target content differences found: rev = 1 action = branch type = text depotFile = //depot/main/applications/util/Utils.java
    
Then please also send the following:

A Revision Graph screen shot from the source server showing the specified file around the changelist which is being replicated. If
an integration is involved then it is important to show the source of the integration.

Filelog output for the file in the source Perforce repository, and filelog output for the source of the integrate being performed.
e.g.

    p4 -ztag filelog /work/p4transfer/main/applications/util/Utils.java@12412
    p4 -ztag filelog /work/p4transfer/dev/applications/util/Utils.java@12412

where 12412 is the changelist number being replicated when the problem occurred.

## Re-running P4Transfer after an error

When an error has been fixed, you can usually re-start P4Transfer from where it left off. If the error occurred when validating changelist 
say 4253 on the target (which was say 12412 on the source) but found to be incorrect, the process is:

    p4 -p target-p4:1666 -u transfer_user -c transfer_workspace obliterate //transfer_workspace/...@4253,4253
    
    (re-run the above with the -y flag to actually perform the obliterate)

Ensure that the counter specified in your config file is set to a value less than 4253 such as the changelist
immediately prior to that changelist.
Then re-run P4Transfer as previously.
