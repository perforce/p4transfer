#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (c) 2011-2021 Sven Erik Knop/Robert Cowham, Perforce Software Ltd
# ========================================
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are
# met:
#
# 1.  Redistributions of source code must retain the above copyright
#     notice, this list of conditions and the following disclaimer.
#
# 2.  Redistributions in binary form must reproduce the above copyright
#     notice, this list of conditions and the following disclaimer in the
#     documentation and/or other materials provided with the
#     distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
# A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL PERFORCE
# SOFTWARE, INC. BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
# SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
# LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
# DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
# THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#

"""
NAME:
    P4Transfer.py

DESCRIPTION:
    This python script (2.7/3.6+ compatible) will transfer Perforce changelists with all contents
    between independent servers when no remote depots are possible, and P4 DVCS commands
    (such as p4 clone/fetch/zip/unzip) are not an option.

    This script transfers changes in one direction - from a source server to a target server.

    Usage:

        python3 P4Transfer.py -h

    The script requires a config file, by default transfer.yaml,
    that provides the Perforce connection information for both servers.

    An initial example can be generated, e.g.

        P4Transfer.py --sample-config > transfer.yaml

    For full documentation/usage, see project doc:

        https://github.com/perforce/p4transfer/blob/main/doc/P4Transfer.adoc

"""

from __future__ import print_function, division

import sys
import re
import hashlib
import stat
import pprint
import errno
from string import Template
import argparse
import textwrap
import os.path
from datetime import datetime
import logging
import time

# Non-standard modules
import P4
import logutils

# Import yaml which will roundtrip comments
from ruamel.yaml import YAML
yaml = YAML()

VERSION = """$Id$"""


def logrepr(self):
    return pprint.pformat(self.__dict__, width=240)


alreadyLogged = {}


# Log messages just once per run
def logOnce(logger, *args):
    global alreadyLogged
    msg = ", ".join([str(x) for x in args])
    if msg not in alreadyLogged:
        alreadyLogged[msg] = 1
        logger.debug(msg)


P4.Revision.__repr__ = logrepr
P4.Integration.__repr__ = logrepr
P4.DepotFile.__repr__ = logrepr
# Old to new typemaps
canonicalTypes = {
    "xtext":    "text+x",
    "ktext":    "text+k",
    "kxtext":   "text+kx",
    "xbinary":  "binary+x",
    "ctext":    "text+C",
    "cxtext":   "text+Cx",
    "ltext":    "text+F",
    "xltext":   "text+Fx",
    "ubinary":  "binary+F",
    "uxbinary": "binary+Fx",
    "tempobj":  "binary+FSw",
    "ctempobj": "binary+Sw",
    "xtempobj": "binary+FSwx",
    "xunicode":  "unicode+x",
    "xutf16":    "utf16+x"
    }

python3 = sys.version_info[0] >= 3
if sys.hexversion < 0x02070000 or (0x0300000 <= sys.hexversion < 0x0303000):
    sys.exit("Python 2.7 or 3.3 or newer is required to run this program.")

# Although this should work with Python 3, it doesn't currently handle Windows Perforce servers
# with filenames containing charaters such as umlauts etc: åäö


class P4TException(Exception):
    pass


class P4TLogicException(P4TException):
    pass


class P4TConfigException(P4TException):
    pass


CONFIG_FILE = 'transfer.yaml'
GENERAL_SECTION = 'general'
SOURCE_SECTION = 'source'
TARGET_SECTION = 'target'
LOGGER_NAME = "P4Transfer"
CHANGE_MAP_DESC = "Updated change_map_file"

# This is for writing to sample config file
DEFAULT_CONFIG = yaml.load(r"""
# counter_name: Unique counter on target server to use for recording source changes processed. No spaces.
#    Name sensibly if you have multiple instances transferring into the same target p4 repository.
#    The counter value represents the last transferred change number - script will start from next change.
#    If not set, or 0 then transfer will start from first change.
counter_name: p4transfer_counter

# case_sensitive: Set this to True if source/target servers are both case sensitive.
#    Otherwise case inconsistencies can cause problems when conversion runs on Linux
case_sensitive: True

# historical_start_change: Set this if you require P4Transfer to start with this changelist.
#    A historical start is useful if you have 100,000 changelists in source server and want to only
#    transfer the last 10,000. Set this value to the first change to be transferred.
#    Once you have set this value and started a transfer DO NOT MODIFY IT or you will potentially
#    mess up integration history etc!!!!!
#    IMPORTANT NOTE: setting this value causes extra work to be done for every integration to adjust
#    revision ranges - thus slowing down transfers.
#    If not set, or 0 then transfer starts from the value of counter_name above, and assumes that ALL HISTORY
#    of included files is transferred.
historical_start_change:

# instance_name: Name of the instance of P4Transfer - for emails etc. Spaces allowed.
instance_name: "Perforce Transfer from XYZ"

# For notification - if smtp not available - expects a pre-configured nms FormMail script as a URL
#   E.g. expects to post using 2 fields: subject, message
# Alternatively, use the following entries (suitable adjusted) to use Mailgun for notifications
#   api: "<Mailgun API key"
#   url: "https://api.mailgun.net/v3/<domain or sandbox>"
#   mail_from: "Fred <fred@example.com>"
#   mail_to:
#   - "fred@example.com"
mail_form_url:

# The mail_* parameters must all be valid (non-blank) to receive email updates during processing.
# mail_to: One or more valid email addresses - comma separated for multiple values
#     E.g. somebody@example.com,somebody-else@example.com
mail_to:

# mail_from: Email address of sender of emails, E.g. p4transfer@example.com
mail_from:

# mail_server: The SMTP server to connect to for email sending, E.g. smtpserver.example.com
mail_server:

# ===============================================================================
# Note that for any of the following parameters identified as (Integer) you can specify a
# valid python expression which evaluates to integer value, e.g.
#     "24 * 60"
#     "7 * 24 * 60"
# Such values should be quoted (in order to be treated as strings)
# -------------------------------------------------------------------------------
# sleep_on_error_interval (Integer): How long (in minutes) to sleep when error is encountered in the script
sleep_on_error_interval: 60

# poll_interval (Integer): How long (in minutes) to wait between polling source server for new changes
poll_interval: 60

# change_batch_size (Integer): changelists are processed in batches of this size
change_batch_size: 1000

# The following *_interval values result in reports, but only if mail_* values are specified
# report_interval (Integer): Interval (in minutes) between regular update emails being sent
report_interval: 30

# error_report_interval (Integer): Interval (in minutes) between error emails being sent e.g. connection error
#     Usually some value less than report_interval. Useful if transfer being run with --repeat option.
error_report_interval: 15

# summary_report_interval (Integer): Interval (in minutes) between summary emails being sent e.g. changes processed
#     Typically some value such as 1 week (10080 = 7 * 24 * 60). Useful if transfer being run with --repeat option.
summary_report_interval: "7 * 24 * 60"

# sync_progress_size_interval (Integer): Size in bytes controlling when syncs are reported to log file.
#    Useful for keeping an eye on progress for large syncs over slow network links.
sync_progress_size_interval: "500 * 1000 * 1000"

# max_logfile_size (Integer): Max size of file to (in bytes) after which it should be rotated
#     Typically some value such as 20MB = 20 * 1024 * 1024. Useful if transfer being run with --repeat option.
max_logfile_size: "20 * 1024 * 1024"

# change_description_format: The standard format for transferred changes.
#    Keywords prefixed with $. Use \\n for newlines. Keywords allowed:
#     $sourceDescription, $sourceChange, $sourcePort, $sourceUser
change_description_format: \"$sourceDescription\\n\\nTransferred from p4://$sourcePort@$sourceChange\"

# change_map_file: Name of an (optional) CSV file listing mappings of source/target changelists.
#    If this is blank (DEFAULT) then no mapping file is created.
#    If non-blank, then a file with this name in the target workspace is appended to
#    and will be submitted after every sequence (batch_size) of changes is made.
#    Default type of this file is text+CS32 to avoid storing too many revisions.
#    File must be mapped into target client workspace.
#    File can contain a sub-directory, e.g. change_map/change_map.csv
#    Note that due to the way client workspace views are created the local filename
#    should include a valid source path including depot name, e.g.
#       //depot/export/... -> depot/export/change_map.csv
change_map_file:

# superuser: Set to n if not a superuser (so can't update change times - can just transfer them).
superuser: "y"

# ignore_files: An array of regex patterns which are used to ingore any matching files.
#    Allows you to ignore some issues which cause transfer problems.
#    E.g.
#    ignore_files:
#    - "some/files/to/*ignore$"
ignore_files:

source:
    # P4PORT to connect to, e.g. some-server:1666 - if this is on localhost and you just
    # want to specify port number, then use quotes: "1666"
    p4port:
    # P4USER to use
    p4user:
    # P4CLIENT to use, e.g. p4-transfer-client
    p4client:
    # P4PASSWD for the user - valid password. If blank then no login performed.
    # Recommended to make sure user is in a group with a long password timeout!.
    # Make sure your P4TICKETS file is correctly found in the environment
    p4passwd:
    # P4CHARSET to use, e.g. none, utf8, etc - leave blank for non-unicode p4d instance
    p4charset:

target:
    # P4PORT to connect to, e.g. some-server:1666 - if this is on localhost and you just
    # want to specify port number, then use quotes: "1666"
    p4port:
    # P4USER to use
    p4user:
    # P4CLIENT to use, e.g. p4-transfer-client
    p4client:
    # P4PASSWD for the user - valid password. If blank then no login performed.
    # Recommended to make sure user is in a group with a long password timeout!
    # Make sure your P4TICKETS file is correctly found in the environment
    p4passwd:
    # P4CHARSET to use, e.g. none, utf8, etc - leave blank for non-unicode p4d instance
    p4charset:

# workspace_root: Root directory to use for both client workspaces.
#    This will be used to update the client workspace Root: field for both source/target workspaces
#    They must be the same.
#    Make sure there is enough space to hold the largest single changelist that will be transferred!
workspace_root: /work/transfer

# views: An array of source/target view mappings
#    You are not allowed to specify both 'views' and 'stream_views' - leave one or other blank!!
#    Each value is a string - normally quote. Standard p4 wildcards are valid.
#    These values are used to construct the appropriate View: fields for source/target client workspaces
#    It is allowed to have exclusion mappings - by specifying the '-' as first character in 'src'
#    entry - see last example below.
views:
  - src:  "//depot/source_path1/..."
    targ: "//import/target_path1/..."
  - src:  "//depot/source_path2/..."
    targ: "//import/target_path2/..."
  - src:  "-//depot/source_path2/exclude/*.tgz"
    targ: "//import/target_path2/exclude/*.tgz"

# transfer_target_stream: The name of a special target stream to use - IT SHOULD NOT CONTAIN FILES!!
#    This will be setup as a mainline stream, with no sharing and with import+ mappings
#    It is in standard stream name format, e.g. //<depot>/<name> or //<depot>/<mid>/<name>
#    e.g. transfer_target_stream: //targ_streams/transfer_target
transfer_target_stream:

# stream_views: An array of source/target stream view mappings and other record fields.
#    You are not allowed to specify both 'views' and 'stream_views' - leave one or other blank
#    Each src/targ value is a string with '*' p4 wildcards to match stream names (like 'p4 streams //depot/rel*')
#    Multiple wildcards are allowed, but make sure the number of wildcards matches between source and target.
#    Please note that target depots must exist.
#    Target streams will be created as required using the specified type/parent fields.
#    Field 'type:' has allowed values: mainline, development, release
#    Field 'parent:' should specify a suitable parent if you are creating development or release streams.
stream_views:
  - src:  "//streams_src/main"
    targ: "//streams_targ/main"
    type: mainline
    parent: ""
  - src:  "//streams_src2/release*"
    targ: "//streams_targ2/rel*"
    type: mainline
    parent: "//streams_targ2/main"
  - src:  "//src3_streams/*rel*"
    targ: "//targ3_streams/*release*"
    type: mainline
    parent: "//targ3_streams/main"

""")


class SourceTargetTextComparison(object):
    """Decide if source and target servers are similar OS so that text
    files can be compared by size and digest (no line ending differences)"""
    sourceVersion = None
    targetVersion = None
    sourceP4DVersion = None
    targetP4DVersion = None
    caseSensitive = False

    def _getServerString(self, server):
        return server.p4cmd("info", "-s")[0]["serverVersion"]

    def _getOS(self, serverString):
        parts = serverString.split("/")
        return parts[1]

    def _getP4DVersion(self, serverString):
        parts = serverString.split("/")
        return parts[2]

    def setup(self, src, targ, caseSensitive=True):
        self.caseSensitive = caseSensitive
        svrString = self._getServerString(src)
        self.sourceVersion = self._getOS(svrString)
        self.sourceP4DVersion = self._getP4DVersion(svrString)
        svrString = self._getServerString(targ)
        self.targetVersion = self._getOS(svrString)
        self.targetP4DVersion = self._getP4DVersion(svrString)

    def compatible(self):
        if self.sourceVersion:
            # TODO: compare different architectures better - e.g. allow 32 vs 64 bit
            return self.sourceVersion == self.targetVersion
        return False


sourceTargetTextComparison = SourceTargetTextComparison()


def specialMovesSupported():
    # Minor new functionality in 2021.1 (2021.1/2126753)
    # #2095201 (Job #95658, #101217) **
    return sourceTargetTextComparison.sourceP4DVersion > "2021.0"


class UTCTimeFromSource(object):
    """Return offset in minutes to be added to source server timestamp to get valid target server timestamp"""
    utcOffset = 0
    reDate = re.compile(r"([\+\-]*)([0-9]{2})([0-9]{2})")

    def _getOffsetString(self, server):
        # Server date: 2015/07/13 14:52:59 -0700 PDT
        # Server date: 2015/07/13 14:52:59 +0100 BST
        try:
            dt = server.p4cmd("info", "-s")[0]["serverDate"]
            return dt.split()[2]
        except:
            return "0000"

    def _getOffsetValue(self, offsetStr):
        try:
            m = self.reDate.match(offsetStr)
            if m:
                result = int(m.group(2)) * 60 + int(m.group(3))
                if m.group(1) and m.group(1) == '-':
                    result = -result
            return result
        except:
            return 0

    def setup(self, src, offsetString=None):
        if offsetString:
            srcOffset = offsetString
        else:
            srcOffset = self._getOffsetString(src)
        self.utcOffset = -self._getOffsetValue(srcOffset)

    def offsetMins(self):
        return self.utcOffset

    def offsetSeconds(self):
        return self.utcOffset * 60


utcTimeFromSource = UTCTimeFromSource()

def stop_file_exists(filepath):
    """Checks if a stop file exists at the given filepath."""
    return os.path.exists(filepath)
STOP_FILE_NAME = "__stopfile"
STOP_FILE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), STOP_FILE_NAME)


def controlled_sleep(minutes):
    start_time = time.time()
    end_time = start_time + (minutes * 60)
    
    while time.time() < end_time:
        if stop_file_exists(STOP_FILE_PATH):
            # Log or print that we detected the stop file and are breaking out of sleep
            return True  # Indicates sleep was interrupted by stop file
        time.sleep(30)  # Sleep for 30 seconds before checking again

    return False  # Indicates full sleep was completed without interruption



def isText(ftype):
    "If filetype is not text - binary or unicode"
    if re.search("text", ftype):
        return True
    return False


def isKeyTextFile(ftype):
    return isText(ftype) and "k" in ftype


alreadyEscaped = re.compile(r"%25|%23|%40|%2A")


def escapeWildCards(fname):
    m = alreadyEscaped.findall(fname)
    if not m:
        fname = fname.replace("%", "%25")
        fname = fname.replace("#", "%23")
        fname = fname.replace("@", "%40")
        fname = fname.replace("*", "%2A")
    return fname


def fileContentComparisonPossible(ftype):
    "Decides if it is possible to compare size/digest for text files"
    if not isText(ftype):
        return True
    if "k" in ftype:
        return False
    return sourceTargetTextComparison.compatible()


def readContents(fname):
    "Reads file contents appropriate according to type"
    if os.name == "posix" and os.path.islink(fname):
        linktarget = os.readlink(fname)
        linktarget += "\n"
        return linktarget
    flags = "rb"
    with open(fname, flags) as fh:
        contents = fh.read()
    return contents


def writeContents(fname, contents):
    flags = "wb"
    ensureDirectory(os.path.dirname(fname))
    if os.path.exists(fname):
        makeWritable(fname)
    with open(fname, flags) as fh:
        try:
            fh.write(contents)
        except TypeError:
            fh.write(contents.encode())


def ensureDirectory(directory):
    if not os.path.isdir(directory):
        os.makedirs(directory)


def makeWritable(fpath):
    "Make file writable"
    os.chmod(fpath, stat.S_IWRITE + stat.S_IREAD)


def getLocalDigest(fname, blocksize=2**20):
    "Return MD5 digest of file on disk"
    m = hashlib.md5()
    if os.name == "posix" and os.path.islink(fname):
        linktarget = os.readlink(fname)
        linktarget += "\n"
        m.update(linktarget)
        return m.hexdigest()
    with open(fname, "rb") as f:
        while True:
            buf = f.read(blocksize)
            if not buf:
                break
            m.update(buf)
    return m.hexdigest()


# All possible p4 keywords (or at least their prefix - there are various $Date* ones
re_rcs_keywords = re.compile(r"\$Id|\$Header|\$Date|\$Change|\$File|\$Revision|\$Author")


def getKTextDigest(fname):
    "Special calculation for ktext files - ignores lines with keywords in them"
    contents = readContents(fname)
    if python3:
        contents = contents.decode()
    m = hashlib.md5()
    # Optimisation to search on whole file
    if not re_rcs_keywords.search(contents):
        if python3:
            m.update(contents.encode())
        else:
            m.update(contents)
        fileSize = os.path.getsize(fname)
        return fileSize, m.hexdigest()
    lines = contents.split("\n")
    fileSize = 0
    for line in lines:
        if not re_rcs_keywords.search(line):
            if python3:
                m.update(line.encode())
            else:
                m.update(line)
            fileSize += len(line)
    return fileSize, m.hexdigest()


def diskFileContentModified(file):
    fileSize = 0
    digest = ""
    if "symlink" in file.type:
        if os.name == "posix":
            assert(os.path.islink(file.fixedLocalFile))
            linktarget = os.readlink(file.fixedLocalFile)
            linktarget += "\n"
            m = hashlib.md5()
            if python3:
                m.update(linktarget.encode())
            else:
                m.update(linktarget)
            fileSize = len(linktarget)
            digest = m.hexdigest()
        else:
            fileSize = os.path.getsize(file.fixedLocalFile)
            digest = getLocalDigest(file.fixedLocalFile)
    elif fileContentComparisonPossible(file.type):
        try:
            fileSize = os.path.getsize(file.fixedLocalFile)
            digest = getLocalDigest(file.fixedLocalFile)
        except EnvironmentError as e:
            if e.errno == errno.ENOENT:
                return False
            else:
                raise
    elif isKeyTextFile(file.type):
        fileSize, digest = getKTextDigest(file.fixedLocalFile)
    return (fileSize, digest.lower()) != (int(file.fileSize), file.digest.lower())


def p4time(unixtime):
    "Convert time to Perforce format time"
    return time.strftime("%Y/%m/%d:%H:%M:%S", time.localtime(unixtime))


def printSampleConfig():
    "Print defaults from above dictionary for saving as a base file"
    print("")
    print("# Save this output to a file to e.g. transfer.yaml and edit it for your configuration")
    print("")
    yaml.dump(DEFAULT_CONFIG, sys.stdout)
    sys.stdout.flush()


def fmtsize(num):
    for x in ['bytes', 'KB', 'MB', 'GB', 'TB']:
        if num < 1024.0:
            return "%3.1f %s" % (num, x)
        num /= 1024.0


class ChangeRevision:
    "Represents a change - created from P4API supplied information and thus encoding"

    def __init__(self, rev, change, n):
        self.rev = rev
        self.action = change['action'][n]
        self.type = change['type'][n]
        self.depotFile = change['depotFile'][n]
        self.localFile = None
        self.fileSize = 0
        self.digest = ""
        self.fixedLocalFile = None
        self._integrations = []
        if self.action not in ['delete', 'move/delete']:
            if 'fileSize' in change:
                try:
                    self.fileSize = change['fileSize'][n]
                except IndexError:
                    self.fileSize = None
            if 'digest' in change:
                try:
                    self.digest = change['digest'][n]
                except IndexError:
                    self.digest = None

    def updateDigest(self):
        "Update values for ktext files if required - assumes file on disk"
        if not isKeyTextFile(self.type) or not self.fixedLocalFile:
            return  # Leave values as default
        if self.action not in ['delete', 'move/delete']:
            self.fileSize, self.digest = getKTextDigest(self.fixedLocalFile)

    def addIntegrationInfo(self, integ):
        "Add what could be more than one integration"
        self._integrations.append(integ)

    def hasIntegrations(self):
        return len(self._integrations)

    def deleteIntegrations(self, integsToDelete):
        "Delete specified indexes - which are in reverse order"
        for ind in integsToDelete:
            del self._integrations[ind]

    def numIntegrations(self):
        return len(self._integrations)

    def hasMoveIntegrations(self):
        for integ in self._integrations:
            if integ.how in ["moved from", "moved into"]:
                return True
        return False

    def hasOnlyMovedFromIntegrations(self):
        for integ in self._integrations:
            if integ.how not in ["moved from"]:
                return False
        return True

    def hasOnlyIgnoreIntegrations(self):
        for integ in self._integrations:
            if integ.how not in ["ignored"]:
                return False
        return True

    def integrations(self):
        "Yield in reverse order so that we replay correctly"
        for ind, integ in reversed(list(enumerate(self._integrations))):
            yield ind, integ

    def getIntegration(self, index=0):
        "Latest integration"
        return self._integrations[index]

    def depotFileRev(self):
        "Fully specify depot file with rev number"
        return "%s#%s" % (self.depotFile, self.rev)

    def localFileRev(self):
        "Fully specify local file with rev number"
        return "%s#%s" % (self.localFile, self.rev)

    def localIntegSourceFile(self, index=0):
        "Local file without rev specifier"
        return self._integrations[index].localFile

    def localIntegSource(self, index=0):
        "Fully specify local source with start/end of revisions"
        if self._integrations[index].srev == 0:
            return "%s#%d" % (self._integrations[index].localFile, self._integrations[index].erev)
        return "%s#%d,%d" % (self._integrations[index].localFile, self._integrations[index].srev + 1,
                             self._integrations[index].erev)

    def localIntegSyncSource(self, index=0):
        "Fully specify local source with end rev for syncing"
        return "%s#%d" % (self._integrations[index].localFile, self._integrations[index].erev)

    def integSyncSource(self, index=0):
        "Integration source with end rev"
        return "%s#%d" % (self._integrations[index].file, self._integrations[index].erev)

    def integSyncSourceWithoutRev(self, index=0):
        "Integration source without env rev"
        return "%s" % (self._integrations[index].file)

    def setLocalFile(self, localFile):
        self.localFile = localFile
        localFile = localFile.replace("%40", "@")
        localFile = localFile.replace("%23", "#")
        localFile = localFile.replace("%2A", "*")
        localFile = localFile.replace("%25", "%")
        localFile = localFile.replace("/", os.sep)
        self.fixedLocalFile = localFile

    def __repr__(self):
        return 'rev={rev} action={action} type={type} size={size} digest={digest} depotFile={depotfile}' .format(
            rev=self.rev,
            action=self.action,
            type=self.type,
            size=self.fileSize,
            digest=self.digest,
            depotfile=self.depotFile,
        )

    def __hash__(self):
        return hash(self.localFile)

    def canonicalType(self):
        "Translate between old style type and new canonical type"
        if self.type in canonicalTypes:
            return canonicalTypes[self.type]
        return self.type

    def __eq__(self, other, caseSensitive=True):
        "For comparisons between source and target after transfer"
        if caseSensitive:
            if self.localFile != other.localFile:   # Check filename
                return False
        else:
            if self.localFile.lower() != other.localFile.lower():
                return False
        # Purge means filetype +Sn - so no comparison possible
        if self.action == 'purge' or other.action == 'purge':
            return True
        # Can't compare branches of purged files - content is "purged file" so size 11 with fixed digest!
        purgedDigest = "08F48C3930677CB9C7F42E5248D560D4"
        if (self.fileSize == '11' and self.digest == purgedDigest) or (other.fileSize == '11' and other.digest == purgedDigest):
            return True
        if fileContentComparisonPossible(self.type):
            if (self.fileSize, self.digest, self.canonicalType()) != (other.fileSize, other.digest, other.canonicalType()):
                if self.type == 'utf16':
                    if abs(int(self.fileSize) - int(other.fileSize)) < 5:
                        return True
                return False
        return True


class ChangelistComparer(object):
    "Compare two lists of filerevisions"

    def __init__(self, logger, caseSensitive=True):
        self.logger = logger
        self.caseSensitive = caseSensitive

    def listsEqual(self, srclist, targlist, filesToIgnore):
        "Compare two lists of changes, with an ignore list"
        srcfiles = set([chRev.localFile for chRev in srclist if chRev.localFile not in filesToIgnore])
        targfiles = set([chRev.localFile for chRev in targlist])
        if not self.caseSensitive:
            srcfiles = set([escapeWildCards(x.lower()) for x in srcfiles])
            targfiles = set([x.lower() for x in targfiles])
        diffs = srcfiles.difference(targfiles)
        if diffs:
            return (False, "Replication failure: missing elements in target changelist:\n%s" % "\n    ".join([str(r) for r in diffs]))
        srcfiles = set(chRev for chRev in srclist if chRev.localFile not in filesToIgnore)
        targfiles = set(chRev for chRev in targlist)
        diffs = srcfiles.difference(targfiles)
        if diffs:
            # Check for no filesize or digest present - indicating "p4 verify -qu" should be run
            new_diffs = [r for r in diffs if r.fileSize and r.digest]
            if not new_diffs:
                self.logger.debug("Ignoring differences due to lack of fileSize/digest or purged files")
                debugDiffs = [r for r in diffs if not r.fileSize or not r.digest]
                self.logger.debug("Missing deleted elements in target changelist:\n%s" % "\n    ".join([str(r) for r in debugDiffs]))
                return (True, "")
            targlookup = {}
            # Cross check again for case insensitive servers - note that this will update the lists!
            if not self.caseSensitive:
                for chRev in srcfiles:
                    chRev.localFile = chRev.localFile.lower()
                for chRev in targfiles:
                    chRev.localFile = chRev.localFile.lower()
                new_diffs = [r for r in diffs if r.fileSize and r.digest]
                diffs2 = srcfiles.difference(targfiles)
                if not diffs2:
                    return (True, "")
                for chRev in targlist:
                    targlookup[chRev.localFile] = chRev
                # For case insenstive, focus on digest rather than fileSize
                new_diffs = [r for r in diffs2 if r != targlookup[escapeWildCards(r.localFile)] and r.digest != targlookup[escapeWildCards(r.localFile)].digest]
                if not new_diffs:
                    return (True, "")
                return (False, "Replication failure (case insensitive): src/target content differences found\nsrc:%s\ntarg:%s" % (
                    "\n    ".join([str(r) for r in diffs]),
                    "\n    ".join([str(targlookup[r.localFile]) for r in diffs])))
            for chRev in targlist:
                targlookup[chRev.localFile] = chRev
            return (False, "Replication failure: src/target content differences found\nsrc:%s\ntarg:%s" % (
                "\n    ".join([str(r) for r in diffs]),
                "\n    ".join([str(targlookup[r.localFile]) for r in diffs])))
        return (True, "")


class ReportProgress(object):
    "Report overall progress"

    def __init__(self, p4, changes, logger, workspace):
        self.logger = logger
        self.filesToSync = 0
        self.changesToSync = len(changes)
        self.sizeToSync = 0
        self.filesSynced = 0
        self.changesSynced = 0
        self.sizeSynced = 0
        self.previousSizeSynced = 0
        self.sync_progress_size_interval = None     # Set to integer value to get reports
        self.logger.info("Syncing %d changes" % (len(changes)))
        self.logger.info("Finding change sizes")
        self.changeSizes = {}
        for chg in changes:
            sizes = p4.run('sizes', '-s', '//%s/...@%s,%s' % (workspace, chg['change'], chg['change']))
            fcount = int(sizes[0]['fileCount'])
            fsize = int(sizes[0]['fileSize'])
            self.sizeToSync += fsize
            self.filesToSync += fcount
            self.changeSizes[chg['change']] = (fcount, fsize)
        self.logger.info("Syncing filerevs %d, size %s" % (self.filesToSync, fmtsize(self.sizeToSync)))

    def SetSyncProgressSizeInterval(self, interval):
        "Set appropriate"
        if interval:
            self.sync_progress_size_interval = int(interval)

    def ReportChangeSync(self):
        self.changesSynced += 1

    def ReportFileSync(self, fileSize):
        self.filesSynced += 1
        self.sizeSynced += fileSize
        if not self.sync_progress_size_interval:
            return
        if self.sizeSynced > self.previousSizeSynced + self.sync_progress_size_interval:
            self.previousSizeSynced = self.sizeSynced
            syncPercent = 100 * float(self.filesSynced) / float(self.filesToSync)
            sizePercent = 100 * float(self.sizeSynced) / float(self.sizeToSync)
            self.logger.info("Synced %d/%d changes, files %d/%d (%2.1f %%), size %s/%s (%2.1f %%)" % (
                    self.changesSynced, self.changesToSync,
                    self.filesSynced, self.filesToSync, syncPercent,
                    fmtsize(self.sizeSynced), fmtsize(self.sizeToSync),
                    sizePercent))


class P4Base(object):
    "Processes a config"

    section = None
    P4PORT = None
    P4CLIENT = None
    P4CHARSET = None
    P4USER = None
    P4PASSWD = None
    counter = 0
    clientLogged = 0
    matchingStreams = []

    def __init__(self, section, options, p4id):
        self.section = section
        self.options = options
        self.logger = logging.getLogger(LOGGER_NAME)
        self.p4id = p4id
        self.p4 = None
        self.client_logged = 0

    def __str__(self):
        return '[section = {} P4PORT = {} P4CLIENT = {} P4USER = {} P4PASSWD = {} P4CHARSET = {}]'.format(
            self.section,
            self.P4PORT,
            self.P4CLIENT,
            self.P4USER,
            self.P4PASSWD,
            self.P4CHARSET,
            )

    def connect(self, progname):
        self.p4 = P4.P4()
        self.p4.port = self.P4PORT
        self.p4.client = self.P4CLIENT
        self.p4.user = self.P4USER
        self.p4.prog = progname
        self.p4.exception_level = P4.P4.RAISE_ERROR
        self.p4.connect()
        if self.P4CHARSET is not None:
            self.p4.charset = self.P4CHARSET
        if self.P4PASSWD is not None:
            self.p4.password = self.P4PASSWD
            self.p4.run_login()

    def streamMatches(self, srcName, streamName):
        "Decides if stream matches the source view (which may contain wildcards)"
        if "*" not in srcName:
            return srcName == streamName
        reSrc = srcName.replace(r"*", r"(.*)")
        m = re.search(reSrc, streamName)
        if m:
            return True
        return False

    def matchingSourceStreams(self, view):
        "Search for any streams matching the source view expanding p4 wildcards *"
        streams = self.p4.run_streams(view['src'])  # Valid with wildcards
        if not streams:
            raise P4TConfigException("No source streams found matching: '%s'" % view['src'])
        return [x['Stream'] for x in streams]

    def matchSourceTargetStreams(self, views):
        "Search for any target streams matching the source view - only valid if called on p4 source"
        self.matchingStreams = []
        for v in views:
            if "*" not in v['src']:
                self.matchingStreams.append((v['src'], v['targ']))
                continue
            srcStreams = self.matchingSourceStreams(v)
            reSrc = v['src'].replace(r"*", r"(.*)")
            numStars = v['src'].count("*")
            reTarg = v['targ'].replace(r"*", r"\1", 1)
            i = 2
            while i <= numStars:
                reTarg = reTarg.replace(r"*", r"\%d" % i, 1)
                i += 1
            for s in srcStreams:
                targ = re.sub(reSrc, reTarg, s)
                self.matchingStreams.append((s, targ))
        return self.matchingStreams

    def createClientWorkspace(self, isSource, matchingStreams=None):
        "Create or adjust client workspace for source or target"
        clientspec = self.p4.fetch_client(self.p4.client)
        logOnce(self.logger, "orig %s:%s:%s" % (self.p4id, self.p4.client, pprint.pformat(clientspec)))

        self.root = self.options.workspace_root
        clientspec._root = self.root
        clientspec["Options"] = clientspec["Options"].replace("noclobber", "clobber")
        clientspec["Options"] = clientspec["Options"].replace("normdir", "rmdir")
        clientspec["LineEnd"] = "unix"
        clientspec._view = []
        # We create/update our special target stream, and also create any required target streams that don't exist
        if self.options.stream_views:
            if isSource:
                self.matchSourceTargetStreams(self.options.stream_views)
                if self.matchingStreams is None:
                    raise P4TConfigException("No matching src/target streams found: %s" % str(self.options.stream_views))
                for s in self.matchingStreams:
                    src = s[0]
                    srcPath = src.replace('//', '')
                    line = "%s/... //%s/%s/..." % (src, self.p4.client, srcPath)
                    clientspec._view.append(line)
            else:
                transferStream = self.p4.fetch_stream(self.options.transfer_target_stream)
                origStream = dict(transferStream)
                transferStream["Type"] = "mainline"
                transferStream["Paths"] = []
                targStreamsUpdated = False
                for v in self.options.stream_views:
                    for s in matchingStreams:   # Array of tuples passed in
                        src = s[0]
                        targ = s[1]
                        if not self.streamMatches(v['src'], src):
                            continue
                        srcPath = src.replace('//', '')
                        line = "import+ %s/... %s/..." % (srcPath, targ)
                        transferStream["Paths"].append(line)
                        targStream = self.p4.fetch_stream('-t', v['type'], targ)
                        origTargStream = dict(targStream)
                        targStream['Type'] = v['type']
                        if v['parent']:
                            targStream['Parent'] = v['parent']
                        if (origTargStream['Type'] != targStream['Type'] and
                           origTargStream['Parent'] != targStream['Parent']) or \
                           ('Update' not in targStream):  # As in this is a new stream
                            self.p4.save_stream(targStream)
                            targStreamsUpdated = True
                if targStreamsUpdated or (origStream["Type"] != transferStream["Type"] and
                   origStream["Paths"] != transferStream["Paths"]):
                    self.p4.save_stream(transferStream)
                clientspec['Stream'] = self.options.transfer_target_stream
        else:   # Ordinary workspace views which allow exclusions
            exclude = ''
            for v in self.options.views:
                lhs = v['src']
                if lhs[0] == '-':
                    exclude = '-'
                    lhs = lhs[1:]
                srcPath = lhs.replace('//', '')
                if isSource:
                    line = "%s%s //%s/%s" % (exclude, lhs, self.p4.client, srcPath)
                else:
                    line = "%s%s //%s/%s" % (exclude, v['targ'], self.p4.client, srcPath)
                clientspec._view.append(line)

        self.clientmap = P4.Map(clientspec._view)
        self.clientspec = clientspec
        self.p4.save_client(clientspec)
        logOnce(self.logger, "updated %s:%s:%s" % (self.p4id, self.p4.client, pprint.pformat(clientspec)))

        # In a streams target we re-read the client view
        if self.options.stream_views and not isSource:
            clientspec = self.p4.fetch_client(self.p4.client)
            self.clientmap = P4.Map(clientspec._view)

        self.p4.cwd = self.root

        ctr = P4.Map('//"'+clientspec._client+'/..."   "' + clientspec._root + '/..."')
        self.localmap = P4.Map.join(self.clientmap, ctr)
        self.depotmap = self.localmap.reverse()

    def p4cmd(self, *args, **kwargs):
        "Execute p4 cmd while logging arguments and results"
        self.logger.debug(self.p4id, args)
        output = self.p4.run(args, **kwargs)
        self.logger.debug(self.p4id, output)
        self.checkWarnings()
        return output

    def disconnect(self):
        if self.p4:
            self.p4.disconnect()

    def checkWarnings(self):
        if self.p4 and self.p4.warnings:
            self.logger.warning('warning result: {}'.format(str(self.p4.warnings)))

    def resetWorkspace(self):
        self.p4cmd('sync', '//%s/...#none' % self.p4.P4CLIENT)


class TrackedAdd(object):
    """Record class used in MoveTracker"""
    def __init__(self, chRev, deleteDepotFile):
        self.chRev = chRev
        self.deleteDepotFile = deleteDepotFile


class MoveTracker(object):
    """Tracks move/add and move/delete and handles orphans where
    source or target isn't mapped (either outside source workspace or not
    visible due to permissions)."""

    def __init__(self, logger):
        self.logger = logger
        self.adds = {}       # Key is depotFile for matching delete
        self.deletes = {}    # Key is depotFile for delete

    def trackAdd(self, chRev, deleteDepotFile):
        "Remember the move/add or move/delete so we can match them up"
        assert(deleteDepotFile not in self.adds)
        self.adds[deleteDepotFile] = TrackedAdd(chRev, deleteDepotFile)

    def trackDelete(self, chRev):
        "Track a move/delete"
        self.deletes[chRev.depotFile] = chRev

    def getMoves(self, msg):
        "Return orphaned moves, or the move/add from add/delete pairs"
        specialMoves = []
        for depotFile in self.adds:
            if depotFile in self.deletes:
                self.logger.debug("%s: Matched move add/delete '%s'" % (msg, depotFile))
                del self.deletes[depotFile]
            else:
                self.logger.debug("%s: Action move/add changed to add '%s'" % (msg, depotFile))
                if specialMovesSupported():
                    specialMoves.append(self.adds[depotFile].chRev)
                else:
                    self.adds[depotFile].chRev.action = "add"
        results = [self.adds[k].chRev for k in self.adds]
        for k in self.deletes:
            self.logger.debug("%s: Action move/delete changed to delete '%s'" % (msg, k))
            self.deletes[k].action = 'delete'
        results.extend([self.deletes[k] for k in self.deletes])
        return results, specialMoves


class SyncOutput(P4.OutputHandler):
    "Log sync progress"

    def __init__(self, p4id, logger, progress):
        P4.OutputHandler.__init__(self)
        self.p4id = p4id
        self.logger = logger
        self.progress = progress
        self.msgs = []

    def outputStat(self, stat):
        if 'fileSize' in stat:
            self.progress.ReportFileSync(int(stat['fileSize']))
        return P4.OutputHandler.HANDLED

    def outputInfo(self, info):
        self.logger.debug(self.p4id, ":", info)
        return P4.OutputHandler.HANDLED

    def outputMessage(self, msg):
        self.logger.warning(self.p4id, ":sync-msg", msg)
        self.msgs.append(str(msg))
        return P4.OutputHandler.HANDLED


class P4Source(P4Base):
    "Functionality for reading from source Perforce repository"

    def __init__(self, section, options):
        super(P4Source, self).__init__(section, options, 'src')
        self.re_content_translation_failed = re.compile("Translation of file content failed near line [0-9]+ file (.*)")
        self.srcFileLogCache = {}

    def missingChanges(self, counter):
        revRange = '//{client}/...@{rev},#head'.format(client=self.P4CLIENT, rev=counter + 1)
        if sourceTargetTextComparison.sourceP4DVersion > "2017.1":
            # We can be more efficient with 2017.2 or greater servers with changes -r -m
            maxChanges = 0
            if self.options.change_batch_size:
                maxChanges = self.options.change_batch_size
            if self.options.maximum and self.options.maximum < maxChanges:
                maxChanges = self.options.maximum
            args = ['changes', '-l', '-r', '-s', 'submitted']
            if maxChanges > 0:
                args.extend(['-m', maxChanges])
            args.append(revRange)
            self.logger.debug('reading changes: %s' % args)
            changes = self.p4cmd(args)
            self.logger.debug('found %d changes' % len(changes))
        else:
            self.logger.debug('reading changes: %s' % revRange)
            changes = self.p4cmd('changes', '-l', revRange)
            self.logger.debug('found %d changes' % len(changes))
            changes.reverse()
            if self.options.change_batch_size:
                changes = changes[:self.options.change_batch_size]
            if self.options.maximum:
                changes = changes[:self.options.maximum]
        self.logger.debug('processing %d changes' % len(changes))
        return changes

    def abortIfUnsyncableUTF16FilesExist(self, syncCallback, change):
        """Find files which can't be synced"""
        unsyncableFiles = []
        for msg in syncCallback.msgs:
            m = self.re_content_translation_failed.search(msg)
            if m:
                depotFile = self.depotmap.translate(m.group(1))
                assert(depotFile)
                unsyncableFiles.append(depotFile)
        if unsyncableFiles:
            msg = "The following utf16 files cannot be synced. Please consider running 'retype -t binary' on them as \n" + \
                "recommended in http://answers.perforce.com/articles/KB/3117:\n    "
            msg += "    \n".join(["%s@%s,%s" % (x, change, change) for x in unsyncableFiles])
            raise P4TException(msg)

    def adjustHistoricalIntegrations(self, fileRevs):
        """Remove any integration records from before start, and adjust start/end rev ranges"""
        startChange = self.options.historical_start_change
        self.logger.debug("Historical integrations adjustments for %d files" % len(fileRevs))
        for chRev in fileRevs:
            if not chRev.hasIntegrations():
                continue
            integsToDelete = []
            for ind, integ in chRev.integrations():
                # Find the earliest revision valid as of startChange, and then use that to calculate offset
                if integ.file not in self.srcFileLogCache:
                    if integ.how == "moved from":
                        continue
                    srcLogs = self.p4.run_filelog('-m1', "%s@%d" % (integ.file, startChange - 1))
                    if srcLogs and srcLogs[0].revisions:
                        rev = srcLogs[0].revisions[0]
                        if rev.change < startChange:
                            self.srcFileLogCache[integ.file] = rev.rev
                if integ.file not in self.srcFileLogCache:
                    continue
                startRev = self.srcFileLogCache[integ.file]
                offset = startRev - 1
                oldErev = integ.erev
                oldSrev = integ.srev
                integ.erev -= offset
                integ.srev -= offset
                if integ.erev <= 0:
                    integsToDelete.append(ind)
                if integ.srev < 0:
                    integ.srev = 0
                if oldErev != integ.erev or oldSrev != integ.srev:
                    self.logger.debug("Adjusting erev/srev from %d/%d to %d/%d for %s" % (
                        oldErev, oldSrev, integ.erev, integ.srev, integ.file
                    ))
            chRev.deleteIntegrations(integsToDelete)

    def adjustLocalFileCase(self, fileRevs):
        """Adjusts case of local files if synced differently"""
        if self.options.case_sensitive:
            return
        haveList = self.p4.run('have', '//{}/...'.format(self.P4CLIENT))
        localHaveFiles = {}
        for f in haveList:
            k = f['depotFile'].lower()
            localHaveFiles[k] = escapeWildCards(f['path'])
            # localHaveFiles[k] = f['path']
        for f in fileRevs:
            d = f.depotFile.lower()
            if f.localFile and d in localHaveFiles:
                if localHaveFiles[d] != f.localFile:
                    self.logger.debug('LocalCaseChange: %s to %s' % (f.localFile, localHaveFiles[d]))
                    f.setLocalFile(localHaveFiles[d])

    def getChange(self, changeNum):
        """Expects change number as a string, and returns list of filerevs and list of filelog output"""

        self.progress.ReportChangeSync()
        change = self.p4cmd('describe', '-s', changeNum)[0]
        fileRevs = []
        specialMoveRevs = []
        filesToLog = {}
        excludedFiles = []
        movetracker = MoveTracker(self.logger)
        for (n, rev) in enumerate(change['rev']):
            localFile = self.localmap.translate(change['depotFile'][n])
            if localFile and len(localFile) > 0:
                chRev = ChangeRevision(rev, change, n)
                chRev.setLocalFile(localFile)
                if chRev.action in ('branch', 'integrate', 'add', 'delete', 'move/add'):
                    filesToLog[chRev.depotFile] = chRev
                elif chRev.action == 'move/delete':
                    movetracker.trackDelete(chRev)
                else:
                    fileRevs.append(chRev)
            else:
                excludedFiles.append(change['depotFile'][n])
        if excludedFiles:
            self.logger.debug('excluded: %s' % excludedFiles)
        fpaths = ['{}#{}'.format(x.depotFile, x.rev) for x in filesToLog.values()]
        filelogs = []
        if fpaths:
            processedRevs = {}
            # Get 2 filelogs per rev - saves time
            filelogs = self.p4.run_filelog('-i', '-m2', *fpaths)
            if len(filelogs) < 1000:
                self.logger.debug('filelogs: %s' % filelogs)
            else:
                self.logger.debug('filelogs count: %d' % len(filelogs))
            for flog in filelogs:
                if flog.depotFile in filesToLog:
                    chRev = filesToLog[flog.depotFile]
                    if chRev.depotFile in processedRevs:    # Only process once
                        continue
                    processedRevs[chRev.depotFile] = 1
                    revision = flog.revisions[0]
                    if len(revision.integrations) > 0:
                        if not self.options.historical_start_change or revision.change >= self.options.historical_start_change:
                            for integ in revision.integrations:
                                if 'from' in integ.how or integ.how == "ignored":
                                    integ.localFile = self.localmap.translate(integ.file)
                                    chRev.addIntegrationInfo(integ)
                    if chRev.action == 'move/add' or (chRev.action == 'add' and chRev.hasMoveIntegrations()):
                        if not chRev.hasIntegrations():
                            # This is move/add with obliterated source
                            fileRevs.append(chRev)
                        else:
                            # Possible that there is a move/add as well as another integration - rare but happens
                            found = False
                            for integ in revision.integrations:
                                if integ.how == 'moved from':
                                    found = True
                                    movetracker.trackAdd(chRev, integ.file)
                            if not found:
                                self.logger.warning(u"Failed to find integ record for move/add {}".format(flog.depotFile))
                    else:
                        fileRevs.append(chRev)
                # else:
                #     self.logger.error(u"Failed to retrieve filelog for {}#{}".format(flog.depotFile,
                #                       flog.rev))

        moveRevs, specialMoveRevs = movetracker.getMoves("getChange")
        fileRevs.extend(moveRevs)
        syncCallback = SyncOutput(self.p4id, self.logger, self.progress)
        self.p4cmd('sync', '//{}/...@={}'.format(self.P4CLIENT, changeNum), handler=syncCallback)
        for flog in filelogs:
            if flog.depotFile in filesToLog:
                chRev = filesToLog[flog.depotFile]
                chRev.updateDigest()
        self.abortIfUnsyncableUTF16FilesExist(syncCallback, changeNum)  # May raise exception
        self.adjustLocalFileCase(fileRevs)
        if self.options.historical_start_change:  # Extra processing required on integration records
            self.adjustHistoricalIntegrations(fileRevs)
        if specialMovesSupported():
            self.processSpecialMoveRevs(fileRevs, specialMoveRevs, filelogs)
        return fileRevs, specialMoveRevs, filelogs

    def processSpecialMoveRevs(self, fileRevs, specialMoveRevs, filelogs):
        """Find any moves where move/from has an add - otherwise remove them from the list"""
        moveDelList = []
        for moveInd, chRev in enumerate(specialMoveRevs):
            copyInteg = moveInteg = None
            chRev.movePartner = None
            if chRev.numIntegrations() == 2 and chRev.action == "move/add":
                for _, integ in chRev.integrations():
                    if integ.how == "moved from":
                        moveInteg = integ
                    elif integ.how == "copy from":
                        copyInteg = integ
            if not copyInteg or not moveInteg:
                moveDelList.append(moveInd)
                continue
            self.logger.debug("Found potential special move: %s" % chRev.depotFile)
            # Find a matching rev from fileRevs and attach to this move rev
            found = -1
            for n, fRev in enumerate(fileRevs):
                if fRev.depotFile == moveInteg.file and fRev.numIntegrations() == 1 and fRev._integrations[0].how == "branch from":
                    found = n
                    chRev.movePartner = fRev
                    self.logger.debug("Matched special move to: %s" % fRev.depotFile)
                    break
            if found < 0:
                self.logger.debug("Ignoring potential special move")
                moveDelList.append(moveInd)
                continue
            del fileRevs[found]
            found = -1
            for n, fRev in enumerate(fileRevs):
                if fRev.depotFile == chRev.depotFile:
                    found = n
                    break
            if found >= 0:
                del fileRevs[found]
        if moveDelList:
            # Iterate in reverse to avoid changing the numbers by deleting!
            for i in reversed(moveDelList):
                del specialMoveRevs[i]

    def getFirstChange(self):
        """Expects change number as a string, and syncs the first historical change"""
        if not self.options.historical_start_change:  # Extra processing required on integration records
            return
        self.progress.ReportChangeSync()
        syncCallback = SyncOutput(self.p4id, self.logger, self.progress)
        self.p4cmd('sync', '//{}/...@{}'.format(self.P4CLIENT, self.options.historical_start_change), handler=syncCallback)


class P4Target(P4Base):
    "Functionality for transferring changes to target Perforce repository"

    def __init__(self, section, options, src):
        super(P4Target, self).__init__(section, options, 'targ')
        self.src = src
        self.resolveDeleteEncountered = False
        self.re_cant_integ_without_Di = re.compile("can't integrate from .* or use -Di to disregard move")
        self.re_cant_integ_without_d = re.compile("can't delete from .* without -d or -Ds flag")
        self.re_cant_integ_without_i = re.compile(r" can't integrate .* without -i flag")
        self.re_cant_branch_without_Dt = re.compile(r" can't branch from .* without -d or -Dt flag")
        self.re_cant_add_existing_file = re.compile(r" can't add existing file")
        self.re_resolve_skipped = re.compile(r" \- resolve skipped.")
        self.re_must_sync_resolve = re.compile(r" must sync/resolve .* before submitting")
        self.re_resolve_tampered = re.compile(r" tampered with before resolve - edit or revert")
        self.re_edit_of_deleted_file = re.compile(r"warning: edit of deleted file")
        self.re_all_revisions_already_integrated = re.compile(r" all revision\(s\) already integrated")
        self.re_no_revisions_above_that_revision = re.compile(r" no revision\(s\) above that revision.")
        self.re_file_not_on_client = re.compile(r"\- file\(s\) not on client")
        self.re_no_such_file = re.compile(r"\- no such file\(s\)")
        self.re_move_delete_needs_move_add = re.compile(r"move/delete\(s\) must be integrated along with matching move/add\(s\)")
        self.re_file_remapped = re.compile(r" \(remapped from ")
        self.filesToIgnore = []
        self.targStartRevCache = {}

    def formatChangeDescription(self, **kwargs):
        """Format using specified format options - see call in replicateChange"""
        format = self.options.change_description_format
        format = format.replace("\\n", "\n")
        t = Template(format)
        result = t.safe_substitute(**kwargs)
        return result

    def ignoreFile(self, fname):
        "Returns True if file is to be ignored"
        if not self.options.re_ignore_files:
            return False
        for exp in self.options.re_ignore_files:
            if exp.search(fname):
                return True
        return False

    def adjustTargetHistoricalIntegrations(self, chRev):
        """Remove any integration records from before start, and adjust start/end rev ranges
        This is a version like the source function but tweaks target for those times when revs have been purged"""
        if not chRev.hasIntegrations():
            return
        integsToDelete = []
        for ind, integ in chRev.integrations():
            # Check for purged start revs indicating some fudging is required
            srcFile = chRev.localIntegSourceFile(ind)
            if not srcFile:
                continue
            depotFile = self.depotmap.translate(srcFile)
            if not depotFile or len(depotFile) == 0:
                continue
            if depotFile not in self.targStartRevCache:
                # Find earliest revision known about this file on the target if not 1 then adjust
                targLogs = self.p4.run_filelog(depotFile)
                if targLogs and targLogs[0].revisions:
                    startRev = targLogs[0].revisions[-1]
                    if startRev.rev == 1:
                        continue
                    endRev = targLogs[0].revisions[0]
                    self.targStartRevCache[depotFile] = (startRev.rev, endRev.rev)
            if depotFile not in self.targStartRevCache:
                continue
            srev, erev = self.targStartRevCache[depotFile]
            offset = srev - 1
            oldErev = integ.erev
            oldSrev = integ.srev
            rdiff = integ.erev - integ.srev
            integ.erev += offset
            integ.srev += offset
            if integ.erev < srev:
                integsToDelete.append(ind)
            elif integ.erev > erev:
                integ.erev = erev
                integ.srev = erev - rdiff
            if integ.srev < srev:
                integ.srev = srev
            if oldErev != integ.erev or oldSrev != integ.srev:
                self.logger.debug("Adjusting obliterated erev/srev from %d/%d to %d/%d for %s" % (
                    oldErev, oldSrev, integ.erev, integ.srev, depotFile
                ))
        chRev.deleteIntegrations(integsToDelete)

    def processChangeRevs(self, fileRevs, specialMoveRevs, srcFileLogs):
        "Process all revisions in the change"
        self.srcFileLogs = {}
        for f in srcFileLogs:
            self.srcFileLogs[f.depotFile] = f
        numProcessed = 0
        for f in fileRevs:
            numProcessed += 1
            if self.options.reset_connection and numProcessed % self.options.reset_connection == 0:
                self.logger.info("Resetting source connection after %d files processed" % numProcessed)
                self.src.p4.disconnect()
                self.src.p4.connect()
            self.logger.debug('targ: %s' % f)
            self.currentFileContent = None

            if self.options.historical_start_change:
                self.adjustTargetHistoricalIntegrations(f)
            if self.ignoreFile(f.localFile):
                self.logger.warning("Ignoring file: %s#%s" % (f.depotFile, f.rev))
                self.filesToIgnore.append(f.localFile)
            elif f.action == 'edit':
                self.logger.debug('processing:0010 edit')
                self.p4cmd('sync', '-k', f.localFile)
                self.p4cmd('edit', '-t', f.type, f.localFile)
                if self.p4.warnings:
                    # Check for file not present - likely to be a purged or archived previous version
                    self.p4cmd('add', '-ft', f.type, f.fixedLocalFile)
                    self.logger.warning('Edit turned into Add due to previous revision not available')
                if diskFileContentModified(f):
                    self.logger.warning('Resyncing source due to file content changes')
                    self.src.p4cmd('sync', '-f', f.localFileRev())
            elif f.action == 'add' or f.action == 'import':
                if f.hasMoveIntegrations():
                    self.moveAdd(f)
                    if f.numIntegrations() > 1:
                        self.replicateIntegration(f, afterAdd=True)
                elif f.hasIntegrations():
                    self.replicateBranch(f, dirty=True)
                else:
                    self.logger.debug('processing:0020 add')
                    self.p4cmd('add', '-ft', f.type, f.fixedLocalFile)
            elif f.action == 'delete':
                if f.hasIntegrations() and not f.hasOnlyMovedFromIntegrations():
                    self.replicateIntegration(f)
                else:
                    self.logger.debug('processing:0030 delete')
                    self.replicateDelete(f)
            elif f.action == 'purge':
                # special case. Type of file is +S, and source.sync removed the file
                # create a temporary file, it will be overwritten again later
                self.logger.debug('processing:0040 purge')
                writeContents(f.fixedLocalFile, 'purged file')
                self.p4cmd('sync', '-k', f.localFile)
                self.p4cmd('edit', '-t', f.type, f.localFile)
                if self.p4.warnings:
                    self.p4cmd('add', '-ft', f.type, f.fixedLocalFile)
            elif f.action == 'branch':
                self.replicateBranch(f, dirty=False)
            elif f.action == 'integrate':
                self.replicateIntegration(f)
            elif f.action == 'move/add':
                self.moveAdd(f)
                if f.numIntegrations() > 1:
                    self.replicateIntegration(f, afterAdd=True)
            elif f.action == 'archive':
                self.logger.warning("Ignoring archived revision: %s#%s" % (f.depotFile, f.rev))
                self.filesToIgnore.append(f.localFile)
            else:
                raise P4TLogicException('Unknown action: %s for %s' % (f.action, str(f)))
        for f in specialMoveRevs:  # There won't be any if not supported
            self.logger.debug('targ: moves %s' % f)
            # These have to be replayed as something like: p4 copy src/... targ/...
            # So we create/adjust a specific branch spec and use that.
            #
            # Branch Spec:
            #     //depot/inside/main/file1 //depot/inside/rel/file1
            #     //depot/inside/main/file2 //depot/inside/rel/file2
            # 2022-03-03 16:37:29,530:P4Transfer:DEBUG: Found special move: //depot/inside/rel/file2
            # 2022-03-03 16:37:29,530:P4Transfer:DEBUG: Matched special move to: //depot/inside/rel/file1
            if f.numIntegrations() != 1 and f._integrations[0].how != "copy from":
                self.logger.error("Unexpected no of integrations: %d for %s" % (f.numIntegrations(), f.depotFile))
                continue
            targFile1 = self.depotmap.translate(f.localFile)
            srcFile1 = self.depotmap.translate(f._integrations[0].localFile)
            branchInteg = None
            for _, integ in f.movePartner.integrations():
                if integ.how == "branch from":
                    branchInteg = integ
                    break
            if not branchInteg:
                continue
            targFile2 = self.depotmap.translate(f.movePartner.localFile)
            srcFile2 = self.depotmap.translate(branchInteg.localFile)
            branchName = '_p4transfer_branch'
            b = self.p4.fetch_branch(branchName)
            b['View'] = ['"%s" "%s"' % (srcFile1, targFile1),
                         '"%s" "%s"' % (srcFile2, targFile2)]
            self.logger.debug('Branch view: %s' % b['View'])
            self.p4.save_branch(b)
            self.p4cmd("copy", "-b", branchName)

    def fixFileTypes(self, fileRevs, openedFiles):
        """Make sure that all integrated filetypes are correct"""
        revDict = {}
        for chRev in fileRevs:
            revDict[chRev.localFile] = chRev
        for ofile in openedFiles:
            localFile = self.localmap.translate(ofile['depotFile'])
            if localFile and len(localFile) > 0 and localFile in revDict:
                chRev = revDict[localFile]
                if chRev.type != ofile['type']:
                    # Can't just do a reopen to +l if working with a commit/edge environment
                    if '+' in chRev.type and 'l' in chRev.type.split('+')[1]:
                        result = self.p4cmd('reopen', '-t', chRev.type, ofile['depotFile'])
                        if "can't change +l type with reopen; use revert -k and then edit -t to change type." in str(result):
                            self.logger.warning(f"Issue identified with file {ofile['depotFile']} suggesting to use 'revert -k' and type change.")
                            self.p4cmd('revert', '-k', ofile['depotFile'])
                            self.p4cmd('add', '-t', chRev.type, ofile['depotFile'])
                            self.p4cmd('edit', '-t', chRev.type, ofile['depotFile'])
                    else:
                        self.p4cmd('reopen', '-t', chRev.type, ofile['depotFile'])

    def replicateChange(self, fileRevs, specialMoveRevs, srcFileLogs, change, sourcePort):
        """This is the heart of it all. Replicate all changes according to their description"""

        self.renameOfDeletedFileEncountered = False
        self.resolveDeleteEncountered = False
        self.filesToIgnore = []
        self.processChangeRevs(fileRevs, specialMoveRevs, srcFileLogs)
        newChangeId = None

        openedFiles = self.p4cmd('opened')
        lenOpenedFiles = len(openedFiles)
        if lenOpenedFiles > 0:
            self.logger.debug("Opened files: %d" % lenOpenedFiles)
            self.fixFileTypes(fileRevs, openedFiles)
            description = self.formatChangeDescription(
                sourceDescription=change['desc'],
                sourceChange=change['change'], sourcePort=sourcePort,
                sourceUser=change['user'])

            if self.options.nokeywords:
                self.removeKeywords(openedFiles)

            result = None
            try:
                # Debug for larger changelists
                if lenOpenedFiles > 1000:
                    self.logger.debug("About to fetch change")
                chg = self.p4.fetch_change()
                chg['Description'] = description
                if lenOpenedFiles > 1000:
                    self.logger.debug("About to submit")
                result = self.p4.save_submit(chg)
                if lenOpenedFiles > 1000:
                    self.logger.debug("submitted")
                self.logger.debug(self.p4id, result)
                self.checkWarnings()
            except P4.P4Exception as e:
                re_resubmit = re.compile("Out of date files must be resolved or reverted.\n.*p4 submit -c ([0-9]+)")
                m = re_resubmit.search(self.p4.errors[0])
                if m and (self.renameOfDeletedFileEncountered or self.resolveDeleteEncountered):
                    cmd = ['sync']
                    for ofile in openedFiles:
                        cmd.append(ofile['depotFile'])
                    self.logger.debug("Resyncing out of date files")
                    self.p4.run(cmd)
                    result = self.p4cmd("submit", "-c", m.group(1))
                else:  # Check for utf16 type problems and change them to binary to see if that works
                    re_transferProblems = re.compile(r".*fix problems then use 'p4 submit -c ([0-9]+)'.\nSome file\(s\) could not be transferred from client")
                    re_translation = re.compile("Translation of file content failed near line [0-9]+ file (.*)")
                    m = re_transferProblems.search(self.p4.errors[0])
                    if not m:
                        raise e
                    chgNo = m.group(1)
                    if self.p4.warnings and re_translation.search(self.p4.warnings[0]):
                        for w in self.p4.warnings:
                            mf = re_translation.search(w)
                            if mf:
                                self.p4cmd("reopen", "-tbinary", mf.group(1))
                                self.filesToIgnore.append(mf.group(1))
                    result = self.p4cmd("submit", "-c", chgNo) # May cause another exception

            # the submit information can be followed by refreshFile lines
            # need to go backwards to find submittedChange

            a = -1
            while 'submittedChange' not in result[a]:
                a -= 1
            newChangeId = result[a]['submittedChange']
            self.updateChange(change, newChangeId)
            self.reverifyRevisions(result)

        self.logger.info("source = {} : target = {}".format(change['change'], newChangeId))
        self.validateSubmittedChange(newChangeId, fileRevs)
        return newChangeId

    def replicateFirstChange(self, sourcePort):
        """Replicate first change when historical start specified"""

        newChangeId = None
        openedFiles = self.p4cmd('reconcile', '-mead', '//%s/...' % self.p4.client)
        lenOpenedFiles = len(openedFiles)
        if lenOpenedFiles > 0:
            description = self.formatChangeDescription(
                sourceDescription='Replicating historical start change',
                sourceChange=str(self.options.historical_start_change), sourcePort=sourcePort,
                sourceUser='historical')
            result = None
            try:
                # Debug for larger changelists
                if lenOpenedFiles > 1000:
                    self.logger.debug("About to fetch change")
                chg = self.p4.fetch_change()
                chg['Description'] = description
                if lenOpenedFiles > 1000:
                    self.logger.debug("About to submit")
                result = self.p4.save_submit(chg)
                if lenOpenedFiles > 1000:
                    self.logger.debug("submitted")
                self.logger.debug(self.p4id, result)
                self.checkWarnings()
            except P4.P4Exception as e:
                raise e

            # the submit information can be followed by refreshFile lines
            # need to go backwards to find submittedChange
            a = -1
            while 'submittedChange' not in result[a]:
                a -= 1
            newChangeId = result[a]['submittedChange']
            # self.updateChange(change, newChangeId)
        self.logger.info("source = {} : target = {}".format(self.options.historical_start_change, newChangeId))
        return newChangeId

    def validateSubmittedChange(self, newChangeId, srcFileRevs):
        "Check against what was passed in"
        movetracker = MoveTracker(self.logger)
        targFileRevs = []
        filesToLog = {}
        if newChangeId:
            change = self.p4cmd('describe', '-s', newChangeId)[0]
            for (n, rev) in enumerate(change['rev']):
                localFile = self.localmap.translate(change['depotFile'][n])
                if localFile and len(localFile) > 0:
                    chRev = ChangeRevision(rev, change, n)
                    chRev.setLocalFile(localFile)
                    if chRev.action == 'move/add':
                        filesToLog[chRev.depotFile] = chRev
                    elif chRev.action == 'move/delete':
                        movetracker.trackDelete(chRev)
                    else:
                        targFileRevs.append(chRev)
        fpaths = ['{}#{}'.format(x.depotFile, x.rev) for x in filesToLog.values()]
        if fpaths:
            filelogs = self.p4.run_filelog('-m1', *fpaths)
            if filelogs:
                self.logger.debug('filelogs: %s' % filelogs)
            for flog in filelogs:
                chRev = filesToLog[flog.depotFile]
                revision = flog.revisions[0]
                if len(revision.integrations) > 0:
                    for integ in revision.integrations:
                        if 'from' in integ.how or integ.how == "ignored":
                            integ.localFile = self.localmap.translate(integ.file)
                            chRev.addIntegrationInfo(integ)
                            break
                    # Possible that there is a move/add as well as another integration - rare but happens
                    found = False
                    for integ in revision.integrations:
                        if integ.how == 'moved from':
                            found = True
                            movetracker.trackAdd(chRev, integ.file)
        cc = ChangelistComparer(self.logger, caseSensitive=self.options.case_sensitive)
        moveRevs, specialMoveRevs = movetracker.getMoves("validate")
        targFileRevs.extend(moveRevs)
        result = cc.listsEqual(srcFileRevs, targFileRevs, self.filesToIgnore)
        if not result[0]:
            if self.options.ignore_errors:
                self.logger.error(result[1])
            else:
                raise P4TLogicException(result[1])

    def syncf(self, localFile):
        self.p4cmd('sync', '-f', localFile)

    def moveAdd(self, file):
        "Either paired with a move/delete or an orphaned move/add"
        self.logger.debug('processing:0100 move/add')
        doMove = False
        ind = 0
        if file.hasIntegrations():
            while ind < file.numIntegrations():
                if file.getIntegration(ind).how == 'moved from':
                    break
                ind += 1
            assert(ind < file.numIntegrations())
            if file.getIntegration(ind).localFile:
                doMove = True
        if doMove:
            source = file.getIntegration(ind).localFile
            self.p4cmd('sync', '-f', file.localIntegSyncSource(ind))
            output = self.p4cmd('edit', source)
            if len(output) > 1 and self.re_edit_of_deleted_file.search(output[-1]):
                self.renameOfDeletedFileEncountered = True
            if self.p4.warnings and self.re_file_not_on_client.search("\n".join(self.p4.warnings)):
                self.p4cmd('sync', '-f', file.localIntegSourceFile(ind))
                output = self.p4cmd('edit', source)
            if len(output) > 1 and self.re_must_sync_resolve.search(output[-1]):
                # Can happen with historical start and manual BBI imports
                self.p4cmd('sync', source)
                self.p4cmd('resolve', '-ay', source)
            if os.path.exists(file.fixedLocalFile) or os.path.islink(file.fixedLocalFile):
                self.p4cmd('move', '-kf', source, file.localFile)
            else:
                self.p4cmd('move', source, file.localFile)
            if diskFileContentModified(file):
                self.logger.warning('Resyncing source due to file content changes')
                self.src.p4cmd('sync', '-f', file.localFileRev())
        else:
            self.logger.debug('processing:0105 move/add converted to add')
            self.p4cmd('add', '-ft', file.type, file.fixedLocalFile)

    def updateChange(self, change, newChangeId):
        # need to update the user and time stamp - but only if a superuser
        if not self.options.superuser == "y":
            return
        newChange = self.p4.fetch_change(newChangeId)
        newChange._user = change['user']
        # date in change is in epoch time (from -Ztag changes - not from change -o), we are good to go with UTC if target server is in UTC timezone
        newDate = datetime.utcfromtimestamp(int(change['time'])).strftime("%Y/%m/%d %H:%M:%S")
        newChange._date = newDate
        self.p4.save_change(newChange, '-f')

    def reverifyRevisions(self, result):
        revisionsToVerify = ["{file}#{rev},{rev}".format(file=x['refreshFile'], rev=x['refreshRev'])
                             for x in result if 'refreshFile' in x]
        if revisionsToVerify:
            self.p4cmd('verify', '-qv', revisionsToVerify)

    def removeKeywords(self, opened):
        for openFile in opened:
            if self.hasKeyword(openFile["type"]):
                fileType = self.removeKeyword(openFile["type"])
                self.p4cmd('reopen', '-t', fileType, openFile["depotFile"])
                self.logger.debug("targ: Changed type from {} to {} for {}".
                                  format(openFile["type"], fileType, openFile["depotFile"]))

    KTEXT = re.compile(r"(.*)\+([^k]*)k([^k]*)")

    def hasKeyword(self, fileType):
        return(fileType in ["ktext", "kxtext"] or self.KTEXT.match(fileType))

    def removeKeyword(self, fileType):
        if fileType == "ktext":
            newType = "text"
        elif fileType == "kxtext":
            newType = "xtext"
        else:
            m = self.KTEXT.match(fileType)
            newType = m.group(1) + "+" + m.group(2) + m.group(3)

        return newType

    def replicateDelete(self, file):
        """Deletes are generally easy but the edge case is a delete on top of a delete"""
        self.p4cmd('delete', '-v', file.localFile)
        if not self.p4.warnings or not self.re_file_not_on_client.search("\n".join(self.p4.warnings)):
            return
        self.p4cmd('sync', '-k', "%s#1" % file.localFile)
        if self.p4.warnings and self.re_no_such_file.search("\n".join(self.p4.warnings)):
            self.logger.warning("Ignoring deleted rev: %s" % file.localFile)
            self.filesToIgnore.append(file.localFile)
            return
        self.p4cmd('delete', '-v', file.localFile)
        self.resolveDeleteEncountered = True

    def replicateBranch(self, file, dirty=False):
        # An integration where source has been obliterated will not have integrations
        self.logger.debug('replicateBranch')
        if not self.options.ignore_integrations and not file.hasOnlyIgnoreIntegrations() and \
           file.hasIntegrations() and file.getIntegration().localFile:
            afterAdd = False
            if not self.currentFileContent and os.path.exists(file.fixedLocalFile):
                self.currentFileContent = readContents(file.fixedLocalFile)
            for ind, integ in file.integrations():
                # With the above in reverse order, we expect any add to occur first
                if file.getIntegration(ind).how == 'add from':
                    self.logger.debug('processing:0200 add from')
                    afterAdd = True
                    if (file.getIntegration(ind).localFile == file.localFile) or \
                            (file.numIntegrations() > 1):
                        self.doIntegrate(file.localIntegSource(ind), file.localFile)
                        self.p4cmd('add', '-ft', file.type, file.fixedLocalFile)
                    else:
                        # "add from" is rather an odd beast - recreate as move after back out of delete
                        self.p4cmd('sync', file.localIntegSyncSource(ind))
                        self.p4cmd('add', file.getIntegration(ind).localFile)
                        makeWritable(file.fixedLocalFile)
                        os.remove(file.fixedLocalFile)
                        self.p4cmd('move', file.getIntegration(ind).localFile, file.fixedLocalFile)
                    if diskFileContentModified(file):
                        self.src.p4cmd('sync', '-f', file.localFileRev())
                elif afterAdd:
                    self.logger.debug('processing:0210 other integrates')
                    self.replicateIntegration(file, afterAdd=afterAdd, startInd=ind)
                elif file.localFile == file.localIntegSourceFile():
                    # Integrate from same file indicates an undo
                    self.logger.debug('processing:0215 undo')
                    self.p4cmd('undo', "%s#%d" % (file.localFile, file._integrations[ind].erev + 1))
                    if diskFileContentModified(file):
                        if file.action == 'add':
                            self.p4cmd('add', file.localFile)
                        else:
                            self.p4cmd('edit', file.localFile)
                        self.src.p4cmd('sync', '-f', file.localFileRev())
                else:
                    self.logger.debug('processing:0220 integrate')
                    if dirty or ind > 0:
                        flags = ['-t']
                    else:
                        flags = ['-t', '-v']
                    outputDict = self.doIntegrate(file.localIntegSource(ind), file.localFile, flags=flags)
                    edited = False
                    added = False
                    afterAdd = True     # This will fire further integrations
                    if outputDict and 'action' in outputDict and outputDict['action'] == 'delete':
                        self.p4cmd('resolve', '-at', file.localFile)
                        self.p4cmd('add', file.localFile)
                        edited = True
                        added = True
                    if dirty and not edited:
                        self.p4cmd('edit', file.localFile)
                        edited = True
                    # Only if last integration to be processed for this rev and it is an add
                    if added or (ind == 0 and outputDict and outputDict['action'] == 'branch' and
                                 self.integrateContentsChanged(file)) or (diskFileContentModified(file)):
                        if not edited:
                            self.p4cmd('edit', file.localFile)
                        self.src.p4cmd('sync', '-f', file.localFileRev())
        else:
            self.logger.debug('processing:0230 add')
            output = self.p4cmd('add', '-ft', file.type, file.fixedLocalFile)
            if len(output) > 0 and self.re_cant_add_existing_file.search(str(output[-1])):
                self.p4cmd('sync', '-k', file.fixedLocalFile)
                self.p4cmd('edit', '-t', file.type, file.fixedLocalFile)
            if diskFileContentModified(file):
                self.logger.warning('Resyncing add due to file content changes')
                self.src.p4cmd('sync', '-f', file.localFileRev())
            if file.hasIntegrations() and file.hasOnlyIgnoreIntegrations():
                self.logger.debug('processing:0235 ignores')
                for ind, integ in file.integrations():
                    self.doIntegrate(file.localIntegSource(ind), file.localFile)
                    self.p4cmd('resolve', '-ay', file.fixedLocalFile)

    def integrateContentsChanged(self, file):
        "Is the source of integrated different to target"
        fileSize, digest = 0, ""
        if fileContentComparisonPossible(file.type):
            if file.integSyncSourceWithoutRev() not in self.srcFileLogs:
                return False
            filelog = self.srcFileLogs[file.integSyncSourceWithoutRev()]
            # Find revision which is not a delete
            if filelog:
                for rev in filelog.revisions:
                    if "delete" not in rev.action:
                        break
                if rev.digest is not None and rev.fileSize is not None:
                    return (rev.fileSize, rev.digest) != (file.fileSize, file.digest)
                else:
                    return False
            else:
                return False
        return True

    def editFrom(self, file, contents, afterAdd=False):
        "Run merge with edit - if required"
        if not afterAdd:
            self.p4cmd('sync', '-f', file.localFile)    # to avoid tamper checking
        self.doIntegrate(file.localIntegSource(), file.localFile)

        class MyResolver(P4.Resolver):
            "Local resolver to accept edits on merge"
            def __init__(self, logger, contents, ftype):
                self.logger = logger
                self.contents = contents
                self.ftype = ftype

            def resolve(self, mergeData):
                self.logger.debug(mergeData)
                result_path = mergeData.result_path
                if not result_path:
                    result_path = mergeData.your_path
                writeContents(result_path, self.contents)
                return 'ae'

        self.p4.run_resolve(resolver=MyResolver(self.logger, contents, file.type))
        self.logger.debug('resolve -ae')

    def integrateWithFlags(self, srcname, destname, flags):
        "integrate which can be repeated with flags where necessary"
        resultStr = ""
        resultDict = {}
        try:
            cmd = ["integrate"]
            cmd.extend(flags)
            cmd.append(srcname)
            cmd.append(destname)
            output = self.p4cmd(cmd)
            if output:
                if isinstance(output[0], dict):
                    resultDict = output[0]
                else:
                    resultStr = output[0]
        except P4.P4Exception:
            resultStr += "\n".join(self.p4.errors)
        resultStr += "\n".join(self.p4.warnings)
        return (resultDict, resultStr)

    def doIntegrate(self, srcname, destname, flags=None):
        "Perform integrate"
        if flags is None:
            flags = ['-t']
        outputDict = {}
        while 1 == 1:
            outputDict, outputStr = self.integrateWithFlags(srcname, destname, flags)
            if self.re_cant_integ_without_i.search(outputStr) and "-i" not in flags:
                flags.append("-i")
            elif self.re_cant_integ_without_d.search(outputStr) and "-d" not in flags:
                flags.append("-d")
            elif self.re_all_revisions_already_integrated.search(outputStr) and "-f" not in flags:
                flags.append("-f")
            elif self.re_cant_integ_without_Di.search(outputStr) and "-Di" not in flags:
                flags.append('-Di')
            elif self.re_cant_branch_without_Dt.search(outputStr) and "-Dt" not in flags:
                flags.append('-Dt')
            elif self.re_no_revisions_above_that_revision.search(outputStr):
                # Happens rarely when deletions have occurred. If there are source revs specified we try with one less rev
                m = re.search("(.*)#([0-9]+),([0-9]+)$", srcname)
                if m:
                    r1 = int(m.group(2)) - 1
                    r2 = int(m.group(3)) - 1
                    newSrc = "%s#%d,%d" % (m.group(1), r1, r2)
                    self.logger.warning("Trying to integrate previous rev '%s'" % newSrc)
                    srcname = newSrc
                else:
                    break
            elif self.re_file_remapped.search(outputStr) and "-2" not in flags:
                flags.append("-2")
            elif self.re_all_revisions_already_integrated.search(outputStr) and "-f" in flags:
                # Can't integrate a delete on to a delete
                self.logger.warning("Ignoring integrate: %s" % destname)
                self.filesToIgnore.append(destname)
                break
            else:
                break
        return outputDict

    def integrateDelete(self, srcFile, srcInd, destname):
        "Handles all deletes"
        self.logger.debug('processing:0260 delete')
        flags = []
        doCopy = False
        fileIgnored = False
        srcname = srcFile.localIntegSource(srcInd)
        while 1 == 1:
            outputDict, outputStr = self.integrateWithFlags(srcname, destname, flags)
            if self.re_cant_integ_without_d.search(outputStr) and "-d" not in flags:
                flags.append("-d")
            elif self.re_cant_integ_without_Di.search(outputStr) and "-Di" not in flags:
                flags.append('-Di')
            elif self.re_cant_branch_without_Dt.search(outputStr) and "-Dt" not in flags:
                flags.append('-Dt')
            elif self.re_all_revisions_already_integrated.search(outputStr) and "-f" not in flags:
                flags.append("-f")
            elif self.re_no_revisions_above_that_revision.search(outputStr):
                # Happens rarely when deletions have occurred. If there are source revs specified we try with one less rev
                m = re.search("(.*)#([0-9]+),([0-9]+)$", srcname)
                if m:
                    r1 = int(m.group(2)) - 1
                    r2 = int(m.group(3)) - 1
                    newSrc = "%s#%d,%d" % (m.group(1), r1, r2)
                    self.logger.warning("Trying to integrate previous rev '%s'" % newSrc)
                    srcname = newSrc
                else:
                    break
            elif self.re_all_revisions_already_integrated.search(outputStr) and "-f" in flags:
                # Can't integrate a delete on to a delete
                self.logger.warning("Ignoring integrate: %s" % destname)
                self.filesToIgnore.append(destname)
                fileIgnored = True
                break
            elif self.re_move_delete_needs_move_add.search(outputStr):
                doCopy = True
                break
            else:
                break
        if doCopy:
            self.logger.debug('processing:0270 copy of delete')
            self.p4cmd('copy', srcFile.localIntegSyncSource(srcInd), destname)
        elif not fileIgnored:
            if outputStr and self.re_no_such_file.search(outputStr):
                self.logger.debug('processing:0275 delete')
                self.p4cmd('delete', '-v', destname)
            else:
                self.logger.debug('processing:0280 delete')
                self.p4cmd('resolve', '-at')

    def replicateIntegration(self, file, afterAdd=False, startInd=None):
        self.logger.debug('replicateIntegration')

        class EditAcceptTheirs(P4.Resolver):
            """
            Required because of a special 'force' flag which is set if this is done interactively - doesn't
            do the same as if you just resolve -at. Yuk!
            """
            def actionResolve(self, mergeInfo):
                return 'at'

        if not self.options.ignore_integrations and file.hasIntegrations() and file.getIntegration().localFile:
            if not self.currentFileContent and os.path.exists(file.fixedLocalFile):
                self.currentFileContent = readContents(file.fixedLocalFile)
            if startInd is None:
                startInd = file.numIntegrations()
            for ind, integ in file.integrations():
                if ind > startInd:
                    continue
                if integ.how in ['add from', 'moved from']:
                    # assert(afterAdd)
                    continue        # We ignore these
                if integ.localFile is None:
                    # Happens with integrations on top of a move from outside source client view
                    self.logger.warning('Ignoring non existent source file')
                    continue
                self.logger.debug('processing:0300 integ: %s' % integ.how)
                if integ.how == 'edit from':
                    self.logger.debug('processing:0305 edit from')
                    if afterAdd:
                        # Resync source version and then do resolve -ae
                        self.src.p4cmd('sync', '-f', file.localFileRev())
                    self.editFrom(file, self.currentFileContent, afterAdd=afterAdd)
                    if diskFileContentModified(file):
                        self.logger.warning('File edited from but content changed')
                        self.src.p4cmd('sync', '-f', file.localFileRev())
                elif integ.how in ('delete', 'delete from'):
                    self.integrateDelete(file, ind, file.localFile)
                else:
                    self.logger.debug('processing:0310 integrate')
                    editedFrom = False
                    if not afterAdd and file.action != 'delete':
                        self.p4cmd('sync', '-f', file.localFile)    # to avoid tamper checking
                    flags = ['-t']
                    if file.action == 'delete' and integ.how == 'ignored':
                        flags.append('-Rb')
                    integResult = self.doIntegrate(file.localIntegSource(ind), file.localFile, flags)
                    if integ.how == 'copy from':
                        self.logger.debug('processing:0320 copy from')
                        self.p4cmd('resolve', '-at')
                        if not editedFrom and diskFileContentModified(file):
                            self.logger.warning('File copied but content changed')
                            self.p4cmd('edit', file.localFile)
                            self.src.p4cmd('sync', '-f', file.localFileRev())
                        # if afterAdd:
                        #     self.logger.debug('Redoing add to avoid problems after forced integrate')
                        #     self.p4cmd('add', '-d', file.localFile)
                    elif integ.how == 'ignored':
                        self.logger.debug('processing:0330 ignored')
                        if 'action' in integResult and integResult['action'] == 'delete':
                            # Strange case of ignoring a delete - we have to revert and redo
                            self.p4cmd('revert', file.localFile)
                            flags.append('-Rd')
                            integResult = self.doIntegrate(file.localIntegSource(ind), file.localFile, flags)
                            self.p4cmd('resolve', '-ay')
                        else:
                            self.p4cmd('resolve', '-ay')
                            if file.action != 'delete' and ind == 0 and diskFileContentModified(file):
                                # Strange but possible in older servers - but only handled for last integrate
                                # in the bunch, hence ind==0
                                self.p4cmd('edit', file.localFile)
                                self.src.p4cmd('sync', '-f', file.localFileRev())
                    elif integ.how == 'merge from':
                        self.logger.debug('processing:0350 merge from')
                        resolve_result = ""
                        try:
                            output = self.p4cmd('resolve', '-am')
                            if output:
                                resolve_result = output[-1]
                        except P4.P4Exception:
                            resolve_result += "\n".join(self.p4.warnings)
                            resolve_result += "\n".join(self.p4.errors)
                        if self.re_resolve_skipped.search(str(resolve_result)):
                            self.logger.warning('Merge from downgraded to edit from due to resolve problems')
                            # Resync source version and then do resolve -ae
                            if not afterAdd:
                                self.p4cmd('revert', file.localFile)
                            self.editFrom(file, self.currentFileContent, afterAdd=afterAdd)
                            editedFrom = True
                            if afterAdd:
                                self.logger.debug('Redoing add to avoid problems after forced integrate')
                                self.p4cmd('add', '-d', file.localFile)
                        elif self.re_resolve_tampered.search(str(resolve_result)):
                            self.p4cmd('edit', file.localFile)
                            self.src.p4cmd('sync', '-f', file.localFileRev())
                        elif not isinstance(resolve_result, dict) and 'how' in resolve_result:
                            self.logger.error('Unexpected resolve error: %s' % resolve_result)
                        # Validate filesize and md5 digest
                        if not editedFrom and diskFileContentModified(file):
                            self.logger.warning('Merge from downgraded to edit from due to file content changes')
                            # Resync source version and then do resolve -ae
                            if not afterAdd:
                                self.p4cmd('revert', file.localFile)
                            self.editFrom(file, self.currentFileContent)
                            if afterAdd:
                                self.logger.debug('Redoing add to avoid problems after forced integrate')
                                self.p4cmd('add', '-d', file.localFile)
                    elif integ.how == 'branch from':
                        self.logger.debug('processing:0355 branch from - interactive -at')
                        self.p4.run_resolve(resolver=EditAcceptTheirs())
                    else:
                        self.logger.error('Cannot deal with {}'.format(integ))
        else:
            self.logger.debug('processing:0360 ignore integrations')
            if file.hasIntegrations() and file.getIntegration().how in ('delete', 'delete from'):
                self.logger.debug('processing:0370 delete')
                self.p4cmd('delete', '-v', file.localFile)
            elif file.action == 'delete' and file.hasIntegrations() and not file.getIntegration().localFile:
                # We don't attempt to transfer files with a delete revision done by an integration
                # that is not being transferred.
                self.logger.warning("Ignoring deleted revision: %s#%s" % (file.depotFile, file.rev))
                self.filesToIgnore.append(file.localFile)
            elif self.options.historical_start_change and not file.hasIntegrations():
                self.logger.debug('processing:0375 - historical change')
                newAction = 'edit'
                self.p4cmd('sync', '-k', file.localFile)
                if self.p4.warnings and self.re_no_such_file.search("\n".join(self.p4.warnings)):
                    newAction = 'add'
                    # self.src.p4cmd('sync', '-f', file.localFileRev())
                    self.p4cmd('add', '-ft', file.type, file.fixedLocalFile)
                else:
                    self.p4cmd('edit', '-t', file.type, file.localFile)
                    if self.p4.warnings and self.re_file_not_on_client.search("\n".join(self.p4.warnings)):
                        self.p4cmd('add', file.localFile)
                self.logger.debug('processing:0376 %s turned into historical %s' % (file.action, newAction))
                if diskFileContentModified(file):
                    self.src.p4cmd('sync', '-f', file.localFileRev())
            else:
                self.logger.debug('processing:0380 else')
                self.p4cmd('sync', '-k', file.localFile)
                self.p4cmd('edit', file.localFile)
                if diskFileContentModified(file):
                    self.src.p4cmd('sync', '-f', file.localFileRev())

    def getCounter(self):
        "Returns value of counter as integer"
        val = 0
        result = self.p4cmd('counter', self.options.counter_name)
        if result and 'counter' in result[0]:
            val = int(result[0]['value'])
        if val == 0 and self.options.historical_start_change > 0:
            val = self.options.historical_start_change - 1
        return val

    def setCounter(self, value):
        "Set's the counter to specified value"
        self.p4cmd('counter', self.options.counter_name, str(value))

    def initChangeMapFile(self):
        "Initializes the file - once"
        if not self.options.change_map_file:
            return
        fpath = os.path.join(self.root, self.options.change_map_file)
        depotFiles = self.p4cmd('fstat', fpath)
        createFile = False
        if depotFiles:
            if not os.path.exists(fpath):
                self.p4cmd('sync', fpath)
            self.p4cmd('edit', fpath)
        else:
            createFile = True
        if createFile:
            ensureDirectory(os.path.dirname(fpath))
            with open(fpath, "a") as fh:
                fh.write("sourceP4Port,sourceChangeNo,targetChangeNo\n")
            output = self.p4cmd('reconcile', fpath)[0]
            if output['action'] == 'add':
                self.p4cmd('reopen', '-t', 'text+CS32', fpath)
        chg = self.p4.fetch_change()
        fpath = os.path.join(self.root, self.options.change_map_file)
        chg['Description'] = CHANGE_MAP_DESC
        output = self.p4.save_change(chg)[0]
        m = re.search("Change ([0-9]+) created", output)
        if not m:
            raise P4TException("Failed to create changelist")
        chgno = m.group(1)
        self.p4cmd('reopen', '-c', chgno, fpath)[0]

    def updateChangeMap(self, sourceP4Port, sourceChangeNo, targetChangeNo):
        "Store values"
        if not self.options.change_map_file:
            return
        fpath = os.path.join(self.root, self.options.change_map_file)
        if os.path.exists(fpath):
            makeWritable(fpath)
        with open(fpath, "a") as fh:
            fh.write("%s,%s,%s\n" % (sourceP4Port, sourceChangeNo, targetChangeNo))

    def submitChangeMap(self):
        if not self.options.change_map_file:
            return
        fpath = os.path.join(self.root, self.options.change_map_file)
        self.logger.debug("Submitting change_map_file")
        output = self.p4cmd('fstat', fpath)[0]
        chgno = output['change']
        self.p4cmd('submit', '-c', chgno)


def valid_datetime_type(arg_datetime_str):
    """custom argparse type for user datetime values given from the command line"""
    try:
        return datetime.strptime(arg_datetime_str, "%Y/%m/%d %H:%M")
    except ValueError:
        msg = "Given Datetime ({0}) not valid! Expected format, 'YYYY/MM/DD HH:mm'!".format(arg_datetime_str)
        raise argparse.ArgumentTypeError(msg)


class P4Transfer(object):
    "Main transfer class"

    def __init__(self, *args):
        desc = textwrap.dedent(__doc__)
        parser = argparse.ArgumentParser(
            description=desc,
            formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog="Copyright (C) 2012-21 Sven Erik Knop/Robert Cowham, Perforce Software Ltd"
        )

        parser.add_argument('-c', '--config', default=CONFIG_FILE, help="Default is " + CONFIG_FILE)
        parser.add_argument('-n', '--notransfer', action='store_true',
                            help="Validate config file and setup source/target workspaces but don't transfer anything")
        parser.add_argument('-m', '--maximum', default=None, type=int, help="Maximum number of changes to transfer")
        parser.add_argument('--reset-connection', default=None, type=int, help="No of files after which to reset connection (for large changes)")
        parser.add_argument('-k', '--nokeywords', action='store_true', help="Do not expand keywords and remove +k from filetype")
        parser.add_argument('-r', '--repeat', action='store_true',
                            help="Repeat transfer in a loop - for continuous transfer as background task")
        parser.add_argument('-s', '--stoponerror', action='store_true', help="Stop on any error even if --repeat has been specified")
        parser.add_argument('--ignore-errors', action='store_true', help="Ignore changelist comparison errors - just log as Error and carry on")
        parser.add_argument('--sample-config', action='store_true', help="Print an example config file and exit")
        parser.add_argument('-i', '--ignore-integrations', action='store_true', help="Treat integrations as adds and edits")
        parser.add_argument('--end-datetime', type=valid_datetime_type, default=None,
                            help="Time to stop transfers, format: 'YYYY/MM/DD HH:mm' - useful"
                            " for automation runs during quiet periods e.g. run overnight but stop first thing in the morning")
        self.options = parser.parse_args(list(args))
        self.options.sync_progress_size_interval = None

        if self.options.sample_config:
            printSampleConfig()
            return

        self.logger = logutils.getLogger(LOGGER_NAME)
        self.previous_target_change_counter = 0     # Current value

    def getOption(self, section, option_name, default=None):
        result = default
        try:
            if section == GENERAL_SECTION:
                result = self.config[option_name]
            else:
                result = self.config[section][option_name]
        except Exception:
            pass
        return result

    def getIntOption(self, section, option_name, default=None):
        result = default
        val = self.getOption(section, option_name, default)
        if isinstance(val, int):
            return val
        if val:
            try:
                result = int(eval(val))
            except Exception:
                pass
        return result

    def readConfig(self):
        self.config = {}
        try:
            with open(self.options.config) as f:
                self.config = yaml.load(f)
        except Exception as e:
            raise P4TConfigException('Could not read config file %s: %s' % (self.options.config, str(e)))

        errors = []
        self.options.counter_name = self.getOption(GENERAL_SECTION, "counter_name")
        if not self.options.counter_name:
            errors.append("Option counter_name must be specified")
        self.options.case_sensitive = self.getIntOption(GENERAL_SECTION, "case_sensitive", 0)
        self.options.historical_start_change = self.getIntOption(GENERAL_SECTION, "historical_start_change", 0)
        self.options.instance_name = self.getOption(GENERAL_SECTION, "instance_name", self.options.counter_name)
        self.options.mail_form_url = self.getOption(GENERAL_SECTION, "mail_form_url")
        self.options.mail_to = self.getOption(GENERAL_SECTION, "mail_to")
        self.options.mail_from = self.getOption(GENERAL_SECTION, "mail_from")
        self.options.mail_server = self.getOption(GENERAL_SECTION, "mail_server")
        self.options.sleep_on_error_interval = self.getIntOption(GENERAL_SECTION, "sleep_on_error_interval", 60)
        self.options.poll_interval = self.getIntOption(GENERAL_SECTION, "poll_interval", 60)
        self.options.change_batch_size = self.getIntOption(GENERAL_SECTION, "change_batch_size", 1000)
        self.options.report_interval = self.getIntOption(GENERAL_SECTION, "report_interval", 30)
        self.options.error_report_interval = self.getIntOption(GENERAL_SECTION, "error_report_interval", 30)
        self.options.summary_report_interval = self.getIntOption(GENERAL_SECTION, "summary_report_interval", 10080)
        self.options.sync_progress_size_interval = self.getIntOption(
            GENERAL_SECTION, "sync_progress_size_interval")
        self.options.max_logfile_size = self.getIntOption(GENERAL_SECTION, "max_logfile_size", 20 * 1024 * 1024)
        self.options.change_description_format = self.getOption(
            GENERAL_SECTION, "change_description_format",
            "$sourceDescription\n\nTransferred from p4://$sourcePort@$sourceChange")
        self.options.change_map_file = self.getOption(GENERAL_SECTION, "change_map_file", "")
        self.options.superuser = self.getOption(GENERAL_SECTION, "superuser", "y")
        self.options.views = self.getOption(GENERAL_SECTION, "views")
        self.options.transfer_target_stream = self.getOption(GENERAL_SECTION, "transfer_target_stream")
        self.options.stream_views = self.getOption(GENERAL_SECTION, "stream_views")
        self.options.workspace_root = self.getOption(GENERAL_SECTION, "workspace_root")
        self.options.ignore_files = self.getOption(GENERAL_SECTION, "ignore_files")
        if not self.options.views and not self.options.stream_views:
            errors.append("One of options views/stream_views must be specified")
        if not self.options.workspace_root:
            errors.append("Option workspace_root must not be blank")
        if self.options.stream_views and not self.options.transfer_target_stream:
            errors.append("Option transfer_target_stream must be specified if streams are being used")
        self.options.re_ignore_files = []
        if self.options.ignore_files:
            for exp in self.options.ignore_files:
                try:
                    self.options.re_ignore_files.append(re.compile(exp))
                except Exception as e:
                    errors.append("Failed to parse ignore_files: %s" % str(e))
        if errors:
            raise P4TConfigException("\n".join(errors))

        self.source = P4Source(SOURCE_SECTION, self.options)
        self.target = P4Target(TARGET_SECTION, self.options, self.source)

        self.readSection(self.source)
        self.readSection(self.target)

    def readSection(self, p4config):
        if p4config.section in self.config:
            self.readOptions(p4config)
        else:
            raise P4TConfigException('Config file needs section %s' % p4config.section)

    def readOptions(self, p4config):
        self.readOption('P4CLIENT', p4config)
        self.readOption('P4USER', p4config)
        self.readOption('P4PORT', p4config)
        self.readOption('P4PASSWD', p4config, optional=True)
        self.readOption('P4CHARSET', p4config, optional=True)

    def readOption(self, option, p4config, optional=False):
        lcOption = option.lower()
        if lcOption in self.config[p4config.section]:
            p4config.__dict__[option] = self.config[p4config.section][lcOption]
        elif not optional:
            raise P4TConfigException('Required option %s not found in section %s' % (option, p4config.section))

    def revertOpenedFiles(self):
        "Clear out any opened files from previous errors - hoping they are transient - except for change_map"
        if not self.options.change_map_file:
            with self.target.p4.at_exception_level(P4.P4.RAISE_NONE):
                self.target.p4cmd('revert', "//%s/..." % self.target.P4CLIENT)
            return
        openChanges = self.target.p4cmd('changes', '-s', 'pending', '-c', self.target.P4CLIENT)
        for change in openChanges:
            if not change['desc'].startswith(CHANGE_MAP_DESC):
                with self.target.p4.at_exception_level(P4.P4.RAISE_NONE):
                    self.target.p4cmd('revert', "-c", change['change'], "//%s/..." % self.target.P4CLIENT)

    def replicate_changes(self):
        "Perform a replication loop"
        self.source.connect('source replicate')
        self.target.connect('target replicate')
        utcTimeFromSource.setup(self.source)
        self.logger.info("UTCTimeFromSource offset (mins) %d" % utcTimeFromSource.offsetMins())
        self.source.createClientWorkspace(True)
        self.target.createClientWorkspace(False, self.source.matchingStreams)
        counterVal = self.target.getCounter()
        changes = self.source.missingChanges(counterVal)
        if self.options.notransfer:
            self.logger.info("Would transfer %d changes - stopping due to --notransfer" % len(changes))
            return 0
        self.logger.info("Transferring %d changes" % len(changes))
        if self.options.historical_start_change and counterVal < self.options.historical_start_change:
            self.logger.info("Transferring first historical change: %d" % self.options.historical_start_change)
            self.source.progress = ReportProgress(self.source.p4, changes, self.logger, self.source.P4CLIENT)
            self.source.progress.SetSyncProgressSizeInterval(self.options.sync_progress_size_interval)
            self.source.getFirstChange()
            targetChange = self.target.replicateFirstChange(self.source.p4.port)
            self.target.setCounter(self.options.historical_start_change)
            # We may have already transferred the first change
            if changes and int(changes[0]['change']) == self.options.historical_start_change:
                del changes[0]
        changesTransferred = 0
        if len(changes) > 0:
            self.target.initChangeMapFile()
            self.save_previous_target_change_counter()
            self.source.progress = ReportProgress(self.source.p4, changes, self.logger, self.source.P4CLIENT)
            self.source.progress.SetSyncProgressSizeInterval(self.options.sync_progress_size_interval)
            self.checkRotateLogFile()
            self.revertOpenedFiles()
            for change in changes:
                if self.endDatetimeExceeded():
                    # Bail early
                    self.logger.info("Transfer stopped due to --end-datetime being exceeded")
                    return changesTransferred
                changeSizes = self.source.progress.changeSizes
                fcount = fsize = 0
                if change['change'] in changeSizes:
                    fcount, fsize = changeSizes[change['change']]
                msg = 'Processing change: {}, files {}, size {} "{}"'.format(
                            change['change'], fcount, fmtsize(fsize), change['desc'].strip())
                self.logger.info(msg)
                fileRevs, specialMoveRevs, srcFileLogs = self.source.getChange(change['change'])
                targetChange = self.target.replicateChange(fileRevs, specialMoveRevs, srcFileLogs, change, self.source.p4.port)
                self.target.setCounter(change['change'])
                self.target.updateChangeMap(self.source.p4.port, change['change'], targetChange)
                # Tidy up the workspaces after successful transfer
                self.source.p4cmd("sync", "//%s/...#0" % self.source.P4CLIENT)
                with self.target.p4.at_exception_level(P4.P4.RAISE_NONE):
                    self.target.p4cmd('sync', "//%s/...#0" % self.target.P4CLIENT)
                changesTransferred += 1
            self.target.submitChangeMap()
        self.source.disconnect()
        self.target.disconnect()
        return changesTransferred

    def log_exception(self, e):
        "Log exceptions appropriately"
        etext = str(e)
        if re.search("WSAETIMEDOUT", etext, re.MULTILINE) or re.search("WSAECONNREFUSED", etext, re.MULTILINE):
            self.logger.error(etext)
        else:
            self.logger.exception(e)

    def save_previous_target_change_counter(self):
        "Save the latest change transferred to the target"
        chg = self.target.p4cmd('changes', '-m1', '-ssubmitted', '//{client}/...'.format(client=self.target.P4CLIENT))
        if chg:
            self.previous_target_change_counter = int(chg[0]['change']) + 1

    def send_summary_email(self, time_last_summary_sent, change_last_summary_sent):
        "Send an email summarising changes transferred"
        time_str = p4time(time_last_summary_sent)
        self.target.connect('target replicate')
        # Combine changes reported by time or since last changelist transferred
        changes = self.target.p4cmd('changes', '-l', '//{client}/...@{rev},#head'.format(
                client=self.target.P4CLIENT, rev=time_str))
        chgnums = [chg['change'] for chg in changes]
        counter_changes = self.target.p4cmd('changes', '-l', '//{client}/...@{rev},#head'.format(
                client=self.target.P4CLIENT, rev=change_last_summary_sent))
        for chg in counter_changes:
            if chg['change'] not in chgnums:
                changes.append(chg)
        changes.reverse()
        lines = []
        lines.append(["Date", "Time", "Changelist", "File Revisions", "Size (bytes)", "Size"])
        total_changes = 0
        total_rev_count = 0
        total_file_sizes = 0
        for chg in changes:
            sizes = self.target.p4cmd('sizes', '-s', '//%s/...@%s,%s' % (self.target.P4CLIENT,
                                                                         chg['change'], chg['change']))
            lines.append([time.strftime("%Y/%m/%d", time.localtime(int(chg['time']))),
                         time.strftime("%H:%M:%S", time.localtime(int(chg['time']))),
                         chg['change'], sizes[0]['fileCount'], sizes[0]['fileSize'],
                         fmtsize(int(sizes[0]['fileSize']))])
            total_changes += 1
            total_rev_count += int(sizes[0]['fileCount'])
            total_file_sizes += int(sizes[0]['fileSize'])
        lines.append([])
        lines.append(['Totals', '', str(total_changes), str(total_rev_count), str(total_file_sizes), fmtsize(total_file_sizes)])
        report = "Changes transferred since %s\n%s" % (
            time_str, "\n".join(["\t".join(line) for line in lines]))
        self.logger.debug("Transfer summary report:\n%s" % report)
        self.logger.info("Sending Transfer summary report")
        self.logger.notify("Transfer summary report", report, include_output=False)
        self.save_previous_target_change_counter()
        self.target.disconnect()

    def validateClientWorkspaces(self):
        if not self.source.root == self.target.root:
            raise P4TConfigException("source and target server workspace root directories must be the same: currently '%s' and '%s'" % (
                self.source.root, self.target.root))
        src = set([m.replace("//%s/" % self.source.P4CLIENT, "") for m in self.source.clientmap.rhs()])
        targ = set([m.replace("//%s/" % self.target.P4CLIENT, "") for m in self.target.clientmap.rhs()])
        diffs = src.difference(targ)
        if diffs and not self.options.stream_views:
            raise P4TConfigException("Configuration failure: workspace mappings have different right hand sides: %s" % ", ".join([str(r) for r in diffs]))
        if self.source.clientspec["LineEnd"] != "unix" or self.target.clientspec["LineEnd"] != "unix":
            raise P4TConfigException("Source and target workspaces must have LineEnd set to 'unix'")
        if re.search("noclobber", self.source.clientspec["Options"]) or re.search("noclobber", self.target.clientspec["Options"]):
            raise P4TConfigException("Source and target workspaces must have 'clobber' option set")

    def validateConfig(self):
        "Performs appropriate validation of config values - primarily streams"
        if self.options.stream_views and not self.options.transfer_target_stream:
            raise P4TConfigException("Option transfer_target_stream is required when stream views are specified")
        if not self.options.stream_views:
            return
        fields = ['src', 'targ', 'type', 'parent']
        reqdFields = ['src', 'targ', 'type']
        types = ['mainline', 'development', 'release']
        errors = []
        for v in self.options.stream_views:
            for f in fields:
                if f not in v:
                    errors.append("Missing required field '%s' in '%s'" % (f, str(v)))
            for f in reqdFields:
                if f in v and not v[f]:
                    errors.append("Required field '%s' must be specified '%s'" % (f, str(v)))
            if 'src' in v and 'targ' in v and v['src'].count("*") != v['targ'].count("*"):
                errors.append("Wildcards need to match src:'%s' targ:'%s'" % (v['src'], v['targ']))
            if 'type' in v and v['type'] not in types:
                errors.append("Stream type '%s' is not one of allowed values '%s'" % (
                              v['type'], " ".join(types)))
        if errors:
            raise P4TConfigException("\n".join(errors))

    def setupReplicate(self):
        "Read config file and setup - raises exceptions if invalid"
        self.readConfig()
        self.source.connect('source replicate')
        self.target.connect('target replicate')
        self.validateConfig()
        self.source.createClientWorkspace(True)
        self.target.createClientWorkspace(False, self.source.matchingStreams)
        self.logger.debug("connected to source and target")
        sourceTargetTextComparison.setup(self.source, self.target, self.options.case_sensitive)
        self.validateClientWorkspaces()

    def writeLogHeader(self):
        "Write header info to log"
        logOnce(self.logger, VERSION)
        logOnce(self.logger, "Python ver: 0x%08x, OS: %s" % (sys.hexversion, sys.platform))
        logOnce(self.logger, "P4Python ver: %s" % (P4.P4.identify()))
        logOnce(self.logger, "Options: ", self.options)
        logOnce(self.logger, "Reading config file")

    def rotateLogFile(self):
        "Rotate existing log file"
        self.logger.info("Rotating logfile")
        logutils.resetLogger(LOGGER_NAME)
        global alreadyLogged
        alreadyLogged = {}
        self.writeLogHeader()

    def checkRotateLogFile(self):
        "Rotate log file if greater than limit"
        try:
            fname = logutils.getCurrentLogFileName(LOGGER_NAME)
            fsize = os.path.getsize(fname)
            if fsize > self.options.max_logfile_size:
                self.logger.info("Rotating logfile since greater than max_logfile_size: %d" % fsize)
                self.rotateLogFile()
        except Exception as e:
            self.log_exception(e)

    def endDatetimeExceeded(self):
        """Determine if we should stop due to this being set"""
        if not self.options.end_datetime:
            return False
        present = datetime.now()
        return present > self.options.end_datetime

    def replicate(self):
        """Central method that performs the replication between server1 and server2"""
        if self.options.sample_config:
            return 0
        try:
            self.writeLogHeader()
            self.setupReplicate()
        except Exception as e:
            self.log_exception(e)
            logging.shutdown()
            return 1

        time_last_summary_sent = time.time()
        change_last_summary_sent = 0
        self.logger.debug("Time last summary sent: %s" % p4time(time_last_summary_sent))
        time_last_error_occurred = 0
        error_encountered = False   # Flag to indicate error encountered which may require reporting
        error_notified = False
        finished = False
        num_changes = 0
        global STOP_FILE_PATH
        STOP_FILE_PATH = os.path.join(os.path.dirname(self.options.config), STOP_FILE_NAME) # Adjust to same dir as config file.
        while not finished:
            try:
                self.readConfig()       # Read every time to allow user to change them
                self.logger.setReportingOptions(
                    instance_name=self.options.instance_name,
                    mail_form_url=self.options.mail_form_url, mail_to=self.options.mail_to,
                    mail_from=self.options.mail_from, mail_server=self.options.mail_server,
                    report_interval=self.options.report_interval)
                logOnce(self.logger, self.source.options)
                logOnce(self.logger, self.target.options)
                logOnce(self.logger, "Stopfile: %s" % STOP_FILE_PATH)
                self.source.disconnect()
                self.target.disconnect()
                num_changes = self.replicate_changes()
                if self.options.notransfer:
                    finished = True
                if num_changes > 0:
                    self.logger.info("Transferred %d changes successfully" % num_changes)
                if change_last_summary_sent == 0:
                    change_last_summary_sent = self.previous_target_change_counter
                if self.options.change_batch_size and num_changes >= self.options.change_batch_size:
                    self.logger.info("Finished processing batch of %d changes" % self.options.change_batch_size)
                    self.rotateLogFile()
                elif not self.options.repeat:
                    finished = True
                else:
                    if self.endDatetimeExceeded():
                        finished = True
                        self.logger.info("Stopping due to --end-datetime parameter being exceeded")
                    if error_encountered:
                        self.logger.info("Logging - reset error interval")
                        self.logger.notify("Cleared error", "Previous error has now been cleared")
                        error_encountered = False
                        error_notified = False
                    if time.time() - time_last_summary_sent > self.options.summary_report_interval * 60:
                        time_last_summary_sent = time.time()
                        self.send_summary_email(time_last_summary_sent, change_last_summary_sent)
                    # Check if controlled_sleep was interrupted by stop file
                    if controlled_sleep(self.options.poll_interval):
                        self.logger.info("Detected stop file. Exiting...")
                        finished = True
                    else:
                        self.logger.info("Sleeping for %d minutes" % self.options.poll_interval)
            except P4TException as e:
                self.log_exception(e)
                self.logger.notify("Error", "Logic Exception encountered - stopping")
                logging.shutdown()
                return 1
            except Exception as e:
                self.log_exception(e)
                if self.options.stoponerror:
                    self.logger.notify("Error", "Exception encountered and --stoponerror specified")
                    logging.shutdown()
                    return 1
                else:
                    # Decide whether to report an error
                    if not error_encountered:
                        error_encountered = True
                        time_last_error_occurred = time.time()
                    elif not error_notified:
                        if time.time() - time_last_error_occurred > self.options.error_report_interval * 60:
                            error_notified = True
                            self.logger.info("Logging - Notifying recurring error")
                            self.logger.notify("Recurring error", "Multiple errors seen")
                    self.logger.info("Sleeping on error for %d minutes" % self.options.sleep_on_error_interval)
                    controlled_sleep(self.options.sleep_on_error_interval)
        self.logger.notify("Changes transferred", "Completed successfully")
        logging.shutdown()
        return 0


if __name__ == '__main__':
    result = 0
    try:
        prog = P4Transfer(*sys.argv[1:])
        result = prog.replicate()
    except Exception as e:
        print(str(e))
        result = 1
    sys.exit(result)
