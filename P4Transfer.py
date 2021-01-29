#!/usr/bin/env python
# -*- coding: utf-8 -*-

#
# Copyright (c) 2011-2014 Sven Erik Knop/Robert Cowham, Perforce Software Ltd
#
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
# User contributed content on the Perforce Public Depot is not supported by Perforce,
# although it may be supported by its author. This applies to all contributions
# even those submitted by Perforce employees.
# ========================================
# P4Transfer.py
#
# This python script will transfer Perforce changelists with all contents
# between independent servers when no remote depots are possible.
#
# This script transfers changes in one direction - from a source server to a target server.
# There is an alternative called PerforceExchange.py which transfers changes
# in both directions.
#
# Usage:
#   python2 P4Transfer.py [options]
#
# The -h/--help option describes all options.
#
# The script requires a config file, normally called transfer.cfg,
# that provides the Perforce connection information for both servers.
# The config file has three sections: [general], [source] and [target].
# See DEFAULT_CONFIG for details. An initial example can be generated, e.g.
#
#   P4Transfer.py --sample-config > transfer.cfg
#
# The script also needs a directory in which it can place the mapped files.
# This directory has to be the root of both servers' workspaces (this will be verified).

from __future__ import print_function

VERSION = """$Id: //guest/perforce_software/p4transfer/P4Transfer.py#112 $"""

import sys, re, hashlib, stat
from collections import OrderedDict
import P4
import pprint
from string import Template

def logrepr(self):
    return pprint.pformat(self.__dict__, width=240)

# Log messages just once per run
alreadyLogged = {}
def logOnce(logger, *args):
    global alreadyLogged
    msg = ", ".join([str(x) for x in args])
    if not msg in alreadyLogged:
        alreadyLogged[msg] = 1
        logger.debug(msg)

P4.Revision.__repr__ = logrepr
P4.Integration.__repr__ = logrepr
P4.DepotFile.__repr__ = logrepr

python3 = sys.version_info[0] >= 3
if sys.hexversion < 0x02070000 or (0x0300000 <= sys.hexversion < 0x0303000):
    sys.exit("Python 2.7 or 3.3 or newer is required to run this program.")

# Although this should work with Python 3, it doesn't currently handle Windows Perforce servers
# with filenames containing charaters such as umlauts etc: åäö
if python3:
    from configparser import ConfigParser
else:
    from ConfigParser import ConfigParser

import argparse
import os.path
from datetime import datetime
import logging
import time

import logutils

class P4TException(Exception):
    pass

class P4TLogicException(Exception):
    pass

class P4TConfigException(P4TException):
    pass

CONFIG = 'transfer.cfg'
GENERAL_SECTION = 'general'
SOURCE_SECTION = 'source'
TARGET_SECTION = 'target'
LOGGER_NAME = "P4Transfer"

# This is for writing to sample config file - OrderedDict used to preserve order of lines.
DEFAULT_CONFIG = OrderedDict({
    GENERAL_SECTION: OrderedDict([
        ("# counter_name: Unique counter on target server to use for recording source changes processed. No spaces.", None),
        ("#    Name sensibly if you have multiple instances transferring into the same target p4 repository.", None),
        ("#    The counter value represents the last transferred change number - script will start from next change.", None),
        ("#    If not set, or 0 then transfer will start from first change.", None),
        ("counter_name", "p4transfer_counter"),
        ("# instance_name: Name of the instance of P4Transfer - for emails etc. Spaces allowed.", None),
        ("instance_name", "Perforce Transfer from XYZ"),
        ("# For notification - if smtp not available - expects a pre-configured nms FormMail script as a URL", None),
        ("mail_form_url", ""),
        ("# The mail_* parameters must all be valid (non-blank) to receive email updates during processing. ", None),
        ("# mail_to: One or more valid email addresses - comma separated for multiple values", None),
        ("#     E.g. somebody@example.com,somebody-else@example.com", None),
        ("mail_to", ""),
        ("# mail_from: Email address of sender of emails, E.g. p4transfer@example.com", None),
        ("mail_from", ""),
        ("# mail_server: The SMTP server to connect to for email sending, E.g. smtpserver.example.com", None),
        ("mail_server", ""),
        ("# ===============================================================================", None),
        ("# Note that for any of the following parameters identified as (Integer) you can specify a", None),
        ("# valid python expression which evaluates to integer value, e.g.", None),
        ("#     24 * 60", None),
        ("#     7 * 24 * 60", None),
        ("# -------------------------------------------------------------------------------", None),
        ("# sleep_on_error_interval (Integer): How long (in minutes) to sleep when error is encountered in the script", None),
        ("sleep_on_error_interval", "60"),
        ("# poll_interval (Integer): How long (in minutes) to wait between polling source server for new changes", None),
        ("poll_interval", "60"),
        ("# change_batch_size (Integer): changelists are processed in batches of this size", None),
        ("change_batch_size", "20000"),
        ("# The following *_interval values result in reports, but only if mail_* values are specified", None),
        ("# report_interval (Integer): Interval (in minutes) between regular update emails being sent", None),
        ("report_interval", "30"),
        ("# error_report_interval (Integer): Interval (in minutes) between error emails being sent e.g. connection error", None),
        ("#     Usually some value less than report_interval. Useful if transfer being run with --repeat option.", None),
        ("error_report_interval", "15"),
        ("# summary_report_interval (Integer): Interval (in minutes) between summary emails being sent e.g. changes processed", None),
        ("#     Typically some value such as 1 week (10080 = 7 * 24 * 60). Useful if transfer being run with --repeat option.", None),
        ("summary_report_interval", "7 * 24 * 60"),
        ("# sync_progress_size_interval (Integer): Size in bytes controlling when syncs are reported to log file. ", None),
        ("#    Useful for keeping an eye on progress for large syncs over slow network links. ", None),
        ("sync_progress_size_interval", "500 * 1000 * 1000"),
        ("# change_description_format: The standard format for transferred changes. ", None),
        ("#    Keywords prefixed with $. Use \\n for newlines. Keywords allowed: ", None),
        ("#     $sourceDescription, $sourceChange, $sourcePort, $sourceUser ", None),
        ("change_description_format", "$sourceDescription\\n\\nTransferred from p4://$sourcePort@$sourceChange"),
        ("# change_map_file: Name of an (optional) CSV file listing mappings of source/target changelists. ", None),
        ("#    If this is blank (DEFAULT) then no mapping file is created.", None),
        ("#    If non-blank, then a file with this name in the target workspace is appended to", None),
        ("#    and will be submitted after every sequence (batch_size) of changes is made.", None),
        ("#    Default type of this file is text+CS32 to avoid storing too many revisions.", None),
        ("#    File must be mapped into target client workspace.", None),
        ("#    File can contain a sub-directory, e.g. change_map/change_map.csv", None),
        ("change_map_file", ""),
        ("# superuser: Set to n if not a superuser (so can't update change times - can just transfer them). ", None),
        ("superuser", "y"),
        ]),
    SOURCE_SECTION: OrderedDict([
        ("# P4PORT to connect to, e.g. some-server:1666", None),
        ("p4port", ""),
        ("# P4USER to use", None),
        ("p4user", ""),
        ("# P4CLIENT to use, e.g. p4-transfer-client", None),
        ("p4client", ""),
        ("# P4PASSWD for the user - valid password. If blank then no login performed.", None),
        ("# Recommended to make sure user is in a group with a long password timeout!.", None),
        ("p4passwd", "")]),
    TARGET_SECTION: OrderedDict([
        ("# P4PORT to connect to, e.g. some-server:1666", None),
        ("p4port", ""),
        ("# P4USER to use", None),
        ("p4user", ""),
        ("# P4CLIENT to use, e.g. p4-transfer-client", None),
        ("p4client", ""),
        ("# P4PASSWD for the user - valid password. If blank then no login performed.", None),
        ("# Recommended to make sure user is in a group with a long password timeout!.", None),
        ("p4passwd", "")]),
    })

class SourceTargetTextComparison(object):
    """Decide if source and target servers are similar OS so that text
    files can be compared by size and digest (no line ending differences)"""
    sourceVersion = None
    targetVersion = None

    def _getOS(self, server):
        info = server.p4cmd("info", "-s")[0]
        serverVersion = info["serverVersion"]
        parts = serverVersion.split("/")
        return parts[1]

    def setup(self, src, targ):
        self.sourceVersion = self._getOS(src)
        self.targetVersion = self._getOS(targ)

    def compatible(self):
        if self.sourceVersion:
            # TODO: compare different architectures better - e.g. allow 32 vs 64 bit
            return self.sourceVersion == self.targetVersion
        return False

sourceTargetTextComparison = SourceTargetTextComparison()

def isText(ftype):
    "If filetype is not text - binary or unicode"
    if re.search("text", ftype):
        return True
    return False

def isKeyTextFile(ftype):
    return isText(ftype) and "k" in ftype

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
        except TypeError as e:
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
re_rcs_keywords = re.compile("\$Id|\$Header|\$Date|\$Change|\$File|\$Revision|\$Author")

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
        fileSize = os.path.getsize(file.fixedLocalFile)
        digest = getLocalDigest(file.fixedLocalFile)
    elif isKeyTextFile(file.type):
        fileSize, digest = getKTextDigest(file.fixedLocalFile)
    return (fileSize, digest.lower()) != (int(file.fileSize), file.digest.lower())

def p4time(unixtime):
    "Convert time to Perforce format time"
    return time.strftime("%Y/%m/%d:%H:%M:%S", time.localtime(unixtime))

def printSampleConfig():
    "Print defaults from above dictionary for saving as a base file"
    config = ConfigParser(allow_no_value=True)
    config.optionxform = str
    for sec in DEFAULT_CONFIG.keys():
        config.add_section(sec)
        for k in DEFAULT_CONFIG[sec].keys():
            config.set(sec, k, DEFAULT_CONFIG[sec][k])
    print("")
    print("# Save this output to a file to e.g. transfer.cfg and edit it for your configuration")
    print("")
    config.write(sys.stdout)
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
            return # Leave values as default
        if self.action not in ['delete', 'move/delete']:
            self.fileSize, self.digest = getKTextDigest(self.fixedLocalFile)

    def addIntegrationInfo(self, integ):
        "Add what could be more than one integration"
        self._integrations.append(integ)

    def hasIntegrations(self):
        return len(self._integrations)

    def numIntegrations(self):
        return len(self._integrations)

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
        if self.type == "xtext":
            return "text+x"
        elif self.type == "ktext":
            return "text+k"
        elif self.type == "kxtext":
            return "text+kx"
        elif self.type == "xbinary":
            return "binary+x"
        elif self.type == "ctext":
            return "text+C"
        elif self.type == "cxtext":
            return "text+Cx"
        elif self.type == "ltext":
            return "text+F"
        elif self.type == "xltext":
            return "text+Fx"
        elif self.type == "ubinary":
            return "binary+F"
        elif self.type == "uxbinary":
            return "binary+Fx"
        elif self.type == "tempobj":
            return "binary+FSw"
        elif self.type == "ctempobj":
            return "binary+Sw"
        elif self.type == "xtempobj":
            return "binary+FSwx"
        elif self.type == "xunicode":
            return "unicode+x"
        elif self.type == "xutf16":
            return "utf16+x"
        else:
            return self.type

    def __eq__(self, other):
        "For comparisons between source and target after transfer"
        if self.localFile != other.localFile:   # Check filename
            return False
        # Purge means filetype +Sn - so no comparison possible
        if self.action == 'purge' or other.action == 'purge':
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

    def __init__(self, logger):
        self.logger = logger

    def listsEqual(self, srclist, targlist, filesToIgnore):
        "Compare two lists of changes, with an ignore list"
        srcfiles = set([chRev.localFile for chRev in srclist if chRev.localFile not in filesToIgnore])
        targfiles = set([chRev.localFile for chRev in targlist])
        diffs = srcfiles.difference(targfiles)
        if diffs:
            return (False, "Replication failure: missing elements in target changelist: %s" % ", ".join([str(r) for r in diffs]))
        srcfiles = set(chRev for chRev in srclist if chRev.localFile not in filesToIgnore)
        targfiles = set(chRev for chRev in targlist)
        diffs = srcfiles.difference(targfiles)
        if diffs:
            # Check for no filesize or digest present - indicating "p4 verify -qu" should be run
            new_diffs = [r for r in diffs if r.fileSize is None or r.digest is None]
            if not new_diffs:
                self.logger.debug("Ignoring differences due to lack of fileSize/digest")
                return (True, "")
            targlookup = {}
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
        for chg in changes:
            sizes = p4.run('sizes', '-s', '//%s/...@%s,%s' % (workspace, chg['change'], chg['change']))
            self.sizeToSync += int(sizes[0]['fileSize'])
            self.filesToSync += int(sizes[0]['fileCount'])
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
            self.logger.info("Synced %d/%d changes, files %d/%d (%2.1f %%), size %s/%s (%2.1f %%)" % ( \
                    self.changesSynced, self.changesToSync,
                    self.filesSynced, self.filesToSync, syncPercent,
                    fmtsize(self.sizeSynced), fmtsize(self.sizeToSync),
                    sizePercent))

class P4Base(object):
    "Processes a config"

    section = None
    P4PORT = None
    P4CLIENT = None
    P4USER = None
    P4PASSWD = None
    counter = 0
    clientLogged = 0

    def __init__(self, section, options, p4id):
        self.section = section
        self.options = options
        self.logger = logging.getLogger(LOGGER_NAME)
        self.p4id = p4id
        self.p4 = None
        self.client_logged = 0

    def __str__(self):
        return '[section = {} P4PORT = {} P4CLIENT = {} P4USER = {} P4PASSWD = {}]'.format( \
            self.section,
            self.P4PORT,
            self.P4CLIENT,
            self.P4USER,
            self.P4PASSWD,
            )

    def connect(self, progname):
        self.p4 = P4.P4()
        self.p4.port = self.P4PORT
        self.p4.client = self.P4CLIENT
        self.p4.user = self.P4USER
        self.p4.prog = progname
        self.p4.exception_level = P4.P4.RAISE_ERROR
        self.p4.connect()
        if not self.P4PASSWD == None:
            self.p4.password = self.P4PASSWD
            self.p4.run_login()

        clientspec = self.p4.fetch_client(self.p4.client)
        logOnce(self.logger, "%s:%s:%s" % (self.p4id, self.p4.client, pprint.pformat(clientspec)))
        self.root = clientspec._root
        self.clientspec = clientspec
        self.p4.cwd = self.root
        self.clientmap = P4.Map(clientspec._view)

        # try:
        #     protects = self.p4cmd("protects", "//%s/..." % clientspec._client)
        # except P4.P4Exception as e:
        #     # This is unlikely except for during testing so ignore it
        #     if e.errors[0] != 'Protections table is empty.':
        #         raise(e)
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

    def trackAdd(self, chRev, addDepotFile, deleteDepotFile):
        "Remember the move/add or move/delete so we can match them up"
        assert(deleteDepotFile not in self.adds)
        self.adds[deleteDepotFile] = TrackedAdd(chRev, deleteDepotFile)

    def trackDelete(self, chRev):
        "Track a move/delete"
        self.deletes[chRev.depotFile] = chRev

    def getMoves(self):
        "Return orphaned moves, or the move/add from add/delete pairs"
        for depotFile in self.adds:
            if depotFile in self.deletes:
                self.logger.debug("Matched move add/delete '%s'" % depotFile)
                del self.deletes[depotFile]
            else:
                self.logger.warning("Action move/add changed to add")
                self.adds[depotFile].chRev.action = "add"
        results = [self.adds[k].chRev for k in self.adds]
        for k in self.deletes:
            self.logger.debug("Action move/delete changed to delete")
            self.deletes[k].action = 'delete'
        results.extend([self.deletes[k] for k in self.deletes])
        return results

class P4Source(P4Base):
    "Functionality for reading from source Perforce repository"

    def __init__(self, section, options):
        super(P4Source, self).__init__(section, options, 'src')
        self.re_content_translation_failed = re.compile("Translation of file content failed near line 1 file (.*)")

    def missingChanges(self, counter):
        revRange = '//{client}/...@{rev},#head'.format(client=self.P4CLIENT, rev=counter + 1)
        self.logger.debug('reading changes: ', revRange)
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

    def getChange(self, change):
        """Expects change number as a string"""

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

        self.progress.ReportChangeSync()
        syncCallback = SyncOutput(self.p4id, self.logger, self.progress)
        self.p4cmd('sync', '//{}/...@{},{}'.format(self.P4CLIENT, change, change), handler=syncCallback)
        self.abortIfUnsyncableUTF16FilesExist(syncCallback, change)  # May raise exception
        change = self.p4cmd('describe', '-s', change)[0]
        filerevs = []
        filesToLog = {}
        excludedFiles = []
        movetracker = MoveTracker(self.logger)
        for (n, rev) in enumerate(change['rev']):
            localFile = self.localmap.translate(change['depotFile'][n])
            if localFile and len(localFile) > 0:
                chRev = ChangeRevision(rev, change, n)
                chRev.setLocalFile(localFile)
                chRev.updateDigest()
                if chRev.action in ('branch', 'integrate', 'add', 'delete', 'move/add'):
                    filesToLog[chRev.depotFile] = chRev
                elif chRev.action == 'move/delete':
                    movetracker.trackDelete(chRev)
                else:
                    filerevs.append(chRev)
            else:
                excludedFiles.append(change['depotFile'][n])
        if excludedFiles:
            self.logger.debug('excluded:', excludedFiles)
        fpaths = ['{}#{}'.format(x.depotFile, x.rev) for x in filesToLog.values()]
        if fpaths:
            filelogs = self.p4.run_filelog('-m1', *fpaths)
            if filelogs:
                self.logger.debug('filelogs:', filelogs)
            for flog in filelogs:
                if flog.depotFile in filesToLog:
                    chRev = filesToLog[flog.depotFile]
                    revision = flog.revisions[0]
                    if len(revision.integrations) > 0:
                        for integ in revision.integrations:
                            if 'from' in integ.how or integ.how == "ignored":
                                integ.localFile = self.localmap.translate(integ.file)
                                chRev.addIntegrationInfo(integ)
                    if chRev.action == 'move/add':
                        if not chRev.hasIntegrations():
                            # This is move/add with obliterated source
                            filerevs.append(chRev)
                        else:
                            self.logger.debug('integration', chRev.getIntegration())
                            movetracker.trackAdd(chRev, flog.depotFile, chRev.getIntegration().file)
                    else:
                        filerevs.append(chRev)
                else:
                    self.logger.error(u"Failed to retrieve filelog for {}#{}".format(flog.depotFile,
                                        flog.rev))
        filerevs.extend(movetracker.getMoves())
        return filerevs

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
        self.re_resolve_skipped = re.compile(" \- resolve skipped.")
        self.re_resolve_tampered = re.compile(" tampered with before resolve - edit or revert")
        self.re_edit_of_deleted_file = re.compile(r"warning: edit of deleted file")
        self.re_all_revisions_already_integrated = re.compile(" all revision\(s\) already integrated")
        self.re_file_not_on_client = re.compile("\- file\(s\) not on client")
        self.re_no_such_file = re.compile("\- no such file\(s\)")
        self.re_move_delete_needs_move_add = re.compile("move/delete\(s\) must be integrated along with matching move/add\(s\)")
        self.re_file_remapped = re.compile(" \(remapped from ")
        self.filesToIgnore = []

    def formatChangeDescription(self, **kwargs):
        """Format using specified format options - see call in replicateChange"""
        format = self.options.change_description_format
        format = format.replace("\\n", "\n")
        t = Template(format)
        result = t.safe_substitute(**kwargs)
        return result

    def processChangeRevs(self, filerevs):
        "Process all revisions in the change"
        for f in filerevs:
            self.logger.debug('targ:', f)
            self.currentFileContent = None

            if f.action == 'edit':
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
                if f.hasIntegrations():
                    self.replicateBranch(f, dirty=True)
                else:
                    self.logger.debug('processing:0020 add')
                    self.p4cmd('add', '-ft', f.type, f.fixedLocalFile)
            elif f.action == 'delete':
                if f.hasIntegrations():
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
            elif f.action == 'archive':
                self.logger.warning("Ignoring archived revision: %s#%s" % (f.depotFile, f.rev))
                self.filesToIgnore.append(f.localFile)
            else:
                raise P4TLogicException('Unknown action: %s for %s' % (f.action, str(f)))

    def fixFileTypes(self, filerevs, openedFiles):
        """Make sure that all integrated filetypes are correct"""
        revDict = {}
        for chRev in filerevs:
            revDict[chRev.localFile] = chRev
        for ofile in openedFiles:
            localFile = self.localmap.translate(ofile['depotFile'])
            if localFile and len(localFile) > 0 and localFile in revDict:
                chRev = revDict[localFile]
                if chRev.type != ofile['type']:
                    self.p4cmd('reopen', '-t', chRev.type, chRev.fixedLocalFile)

    def replicateChange(self, filerevs, change, sourcePort):
        """This is the heart of it all. Replicate all changes according to their description"""

        self.renameOfDeletedFileEncountered = False
        self.resolveDeleteEncountered = False
        self.filesToIgnore = []
        self.processChangeRevs(filerevs)
        newChangeId = None

        openedFiles = self.p4cmd('opened')
        if len(openedFiles) > 0:
            self.fixFileTypes(filerevs, openedFiles)
            description = self.formatChangeDescription(sourceDescription=change['desc'],
                sourceChange=change['change'], sourcePort=sourcePort,
                sourceUser=change['user'])

            if self.options.nokeywords:
                self.removeKeywords(openedFiles)

            result = None
            try:
                chg = self.p4.fetch_change()
                chg['Description'] = description
                result = self.p4.save_submit(chg)
                self.logger.debug(self.p4id, result)
                self.checkWarnings()
            except P4.P4Exception as e:
                re_resubmit = re.compile("Out of date files must be resolved or reverted.\n.*p4 submit -c ([0-9]+)")
                m = re_resubmit.search(self.p4.errors[0])
                if m and (self.renameOfDeletedFileEncountered or self.resolveDeleteEncountered):
                    self.p4cmd("sync")
                    result = self.p4cmd("submit", "-c", m.group(1))
                else:
                    raise e

            # the submit information can be followed by refreshFile lines
            # need to go backwards to find submittedChange

            a = -1
            while 'submittedChange' not in result[a]:
                a -= 1
            newChangeId = result[a]['submittedChange']
            self.updateChange(change, newChangeId)
            self.reverifyRevisions(result)

        self.logger.info("source = {} : target = {}".format(change['change'], newChangeId))
        self.validateSubmittedChange(newChangeId, filerevs)
        return newChangeId

    def validateSubmittedChange(self, newChangeId, srcFileRevs):
        "Check against what was passed in"
        movetracker = MoveTracker(self.logger)
        targFileRevs = []
        filesToLog = {}
        if newChangeId:
            change = self.p4cmd('describe', newChangeId)[0]
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
                self.logger.debug('filelogs:', filelogs)
            for flog in filelogs:
                chRev = filesToLog[flog.depotFile]
                revision = flog.revisions[0]
                if len(revision.integrations) > 0:
                    for integ in revision.integrations:
                        if 'from' in integ.how or integ.how == "ignored":
                            integ.localFile = self.localmap.translate(integ.file)
                            chRev.addIntegrationInfo(integ)
                            break
                movetracker.trackAdd(chRev, flog.depotFile, chRev.getIntegration().file)
        cc = ChangelistComparer(self.logger)
        targFileRevs.extend(movetracker.getMoves())
        result = cc.listsEqual(srcFileRevs, targFileRevs, self.filesToIgnore)
        if not result[0]:
            raise P4TLogicException(result[1])

    def syncf(self, localFile):
        self.p4cmd('sync', '-f', localFile)


    def moveAdd(self, file):
        "Either paired with a move/delete or an orphaned move/add"
        self.logger.debug('processing:0100 move/add')
        if file.hasIntegrations() and file.getIntegration() and file.getIntegration().localFile:
            source = file.getIntegration().localFile
            self.p4cmd('sync', '-f', file.localIntegSyncSource())
            output = self.p4cmd('edit', source)
            if len(output) > 1 and self.re_edit_of_deleted_file.search(output[-1]):
                self.renameOfDeletedFileEncountered = True
            if os.path.exists(file.fixedLocalFile) or os.path.islink(file.fixedLocalFile):
                self.p4cmd('move', '-k', source, file.localFile)
            else:
                self.p4cmd('move', source, file.localFile)
            if diskFileContentModified(file):
                self.logger.warning('Resyncing source due to file content changes')
                self.src.p4cmd('sync', '-f', file.localFileRev())
        else:
            self.p4cmd('add', '-ft', file.type, file.fixedLocalFile)

    def updateChange(self, change, newChangeId):
        # need to update the user and time stamp - but only if a superuser
        if not self.options.superuser == "y":
            return
        newChange = self.p4.fetch_change(newChangeId)
        newChange._user = change['user']
        # date in change is in epoch time, we need it in canonical form
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

    KTEXT = re.compile("(.*)\+([^k]*)k([^k]*)")

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
            self.logger.warning("Ignoring deleted rev:", file.localFile)
            self.filesToIgnore.append(file.localFile)
            return
        self.p4cmd('delete', '-v', file.localFile)
        self.resolveDeleteEncountered = True

    def replicateBranch(self, file, dirty=False):
        # An integration where source has been obliterated will not have integrations
        self.logger.debug('replicateBranch')
        if not self.options.ignore and file.hasIntegrations() and file.getIntegration().localFile:
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
                else:
                    self.logger.debug('processing:0220 integrate')
                    if dirty or ind > 0 :
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
                    if added or (ind == 0 and outputDict and outputDict['action'] == 'branch' and \
                            self.integrateContentsChanged(file)):
                        if not edited:
                            self.p4cmd('edit', file.localFile)
                        self.src.p4cmd('sync', '-f', file.localFileRev())
        else:
            self.logger.debug('processing:0230 add')
            self.p4cmd('add', '-ft', file.type, file.fixedLocalFile)
            if diskFileContentModified(file):
                self.logger.warning('Resyncing add due to file content changes')
                self.src.p4cmd('sync', '-f', file.localFileRev())

    def integrateContentsChanged(self, file):
        "Is the source of integrated different to target"
        fileSize, digest = 0, ""
        if fileContentComparisonPossible(file.type):
            filelog = self.src.p4cmd('filelog', '-m1', file.integSyncSource())
            if filelog and 'digest' in filelog[0] and 'fileSize' in filelog[0]:
                fileSize = filelog[0]['fileSize'][0]
                digest = filelog[0]['digest'][0]
                return (fileSize, digest) != (file.fileSize, file.digest)
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
            elif self.re_file_remapped.search(outputStr) and "-2" not in flags:
                flags.append("-2")
            elif self.re_all_revisions_already_integrated.search(outputStr) and "-f" in flags:
                # Can't integrate a delete on to a delete
                self.logger.warning("Ignoring integrate:", destname)
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
        while 1 == 1:
            outputDict, outputStr = self.integrateWithFlags(srcFile.localIntegSource(srcInd), destname, flags)
            if self.re_cant_integ_without_d.search(outputStr) and "-d" not in flags:
                flags.append("-d")
            elif self.re_cant_integ_without_Di.search(outputStr) and "-Di" not in flags:
                flags.append('-Di')
            elif self.re_cant_branch_without_Dt.search(outputStr) and "-Dt" not in flags:
                flags.append('-Dt')
            elif self.re_all_revisions_already_integrated.search(outputStr) and "-f" not in flags:
                flags.append("-f")
            elif self.re_all_revisions_already_integrated.search(outputStr) and "-f" in flags:
                # Can't integrate a delete on to a delete
                self.logger.warning("Ignoring integrate:", destname)
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

        if not self.options.ignore and file.hasIntegrations() and file.getIntegration().localFile:
            if not self.currentFileContent and os.path.exists(file.fixedLocalFile):
                self.currentFileContent = readContents(file.fixedLocalFile)
            if startInd is None:
                startInd = file.numIntegrations()
            for ind, integ in file.integrations():
                if ind > startInd:
                    continue
                if integ.how == 'add from':
                    assert(afterAdd)
                    continue        # We ignore these
                self.logger.debug('processing:0300 integ:', integ.how)
                if integ.how == 'edit from':
                    self.logger.debug('processing:0305 edit from')
#                    if afterAdd or ind < file.numIntegrations():
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
                    self.doIntegrate(file.localIntegSource(ind), file.localFile, flags)
                    if integ.how == 'copy from':
                        self.logger.debug('processing:0320 copy from')
                        self.p4cmd('resolve', '-at')
                        if not editedFrom and diskFileContentModified(file):
                            self.logger.warning('File copied but content changed')
                            self.src.p4cmd('sync', '-f', file.localFileRev())
                    elif integ.how == 'ignored':
                        self.logger.debug('processing:0330 ignored')
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
            else:
                self.logger.debug('processing:0380 else')
                self.p4cmd('sync', '-k', file.localFile)
                self.p4cmd('edit', file.localFile)
                if diskFileContentModified(file):
                    self.src.p4cmd('sync', '-f', file.localFileRev())

    def getCounter(self):
        "Returns value of counter as integer"
        result = self.p4cmd('counter', self.options.counter_name)
        if result and 'counter' in result[0]:
            return int(result[0]['value'])
        return 0

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
        chg['Description'] = "Updated change_map_file"
        output = self.p4.save_change(chg)[0]
        m = re.search("Change (\d+) created", output)
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
        parser = argparse.ArgumentParser(
            description="P4Transfer",
            epilog="Copyright (C) 2012-14 Sven Erik Knop/Robert Cowham, Perforce Software Ltd"
        )

        parser.add_argument('-c', '--config', default=CONFIG, help="Default is " + CONFIG)
        parser.add_argument('-m', '--maximum', default=None, type=int, help="Maximum number of changes to transfer")
        parser.add_argument('-k', '--nokeywords', action='store_true', help="Do not expand keywords and remove +k from filetype")
        parser.add_argument('-r', '--repeat', action='store_true', help="Repeat transfer in a loop - for continuous transfer")
        parser.add_argument('-s', '--stoponerror', action='store_true', help="Stop on any error even if --repeat has been specified")
        parser.add_argument('--sample-config', action='store_true', help="Print an example config file and exit")
        parser.add_argument('-i', '--ignore', action='store_true', help="Treat integrations as adds and edits")
        parser.add_argument('--end-datetime', type=valid_datetime_type, default=None,
            help="Time to stop transfers, format: 'YYYY/MM/DD HH:mm'")
        self.options = parser.parse_args(list(args))
        self.options.sync_progress_size_interval = None

        if self.options.sample_config:
            printSampleConfig()
            return
            
        self.logger = logutils.getLogger(LOGGER_NAME)
        self.previous_target_change_counter = 0 # Current value

    def getOption(self, section, option_name, default=None):
        result = default
        try:
            result = self.parser.get(section, option_name)
        except:
            pass
        return result

    def getIntOption(self, section, option_name, default=None):
        result = default
        strval = self.getOption(section, option_name, default)
        if strval:
            try:
                result = int(eval(strval))
            except:
                pass
        return result

    def readConfig(self):
        self.parser = ConfigParser()
        self.options.parser = self.parser    # for later use
        try:
            with open(self.options.config) as f:
                if python3:
                    self.parser.read_file(f)
                else:
                    self.parser.readfp(f)
        except Exception as e:
            raise P4TConfigException('Could not read %s: %s' % (self.options.config, str(e)))

        self.options.counter_name = self.getOption(GENERAL_SECTION, "counter_name")
        if not self.options.counter_name:
            raise P4TConfigException("Option counter_name in the [general] section must be specified")
        self.options.instance_name = self.getOption(GENERAL_SECTION, "instance_name", self.options.counter_name)
        self.options.mail_form_url = self.getOption(GENERAL_SECTION, "mail_form_url")
        self.options.mail_to = self.getOption(GENERAL_SECTION, "mail_to")
        self.options.mail_from = self.getOption(GENERAL_SECTION, "mail_from")
        self.options.mail_server = self.getOption(GENERAL_SECTION, "mail_server")
        self.options.sleep_on_error_interval = self.getIntOption(GENERAL_SECTION, "sleep_on_error_interval", 60)
        self.options.poll_interval = self.getIntOption(GENERAL_SECTION, "poll_interval", 60)
        self.options.change_batch_size = self.getIntOption(GENERAL_SECTION, "change_batch_size", 20000)
        self.options.report_interval = self.getIntOption(GENERAL_SECTION, "report_interval", 30)
        self.options.error_report_interval = self.getIntOption(GENERAL_SECTION, "error_report_interval", 30)
        self.options.summary_report_interval = self.getIntOption(GENERAL_SECTION, "summary_report_interval", 10080)
        self.options.sync_progress_size_interval = self.getIntOption(GENERAL_SECTION,
                    "sync_progress_size_interval")
        self.options.change_description_format = self.getOption(GENERAL_SECTION, "change_description_format",
                    "$sourceDescription\n\nTransferred from p4://$sourcePort@$sourceChange")
        self.options.change_map_file = self.getOption(GENERAL_SECTION, "change_map_file", "")
        self.options.superuser = self.getOption(GENERAL_SECTION, "superuser", "y")

        self.source = P4Source(SOURCE_SECTION, self.options)
        self.target = P4Target(TARGET_SECTION, self.options, self.source)

        self.readSection(self.source)
        self.readSection(self.target)

    def readSection(self, p4config):
        if self.parser.has_section(p4config.section):
            self.readOptions(p4config)
        else:
            raise P4TConfigException('Config file needs section %s' % p4config.section)

    def readOptions(self, p4config):
        self.readOption('P4CLIENT', p4config)
        self.readOption('P4USER', p4config)
        self.readOption('P4PORT', p4config)
        self.readOption('P4PASSWD', p4config, optional=True)

    def readOption(self, option, p4config, optional=False):
        if self.parser.has_option(p4config.section, option):
            p4config.__dict__[option] = self.parser.get(p4config.section, option)
        elif not optional:
            raise P4TConfigException('Required option %s not found in section %s' % (option, p4config.section))

    def replicate_changes(self):
        "Perform a replication loop"
        self.source.connect('source replicate')
        self.target.connect('target replicate')
        changes = self.source.missingChanges(self.target.getCounter())
        self.logger.info("Transferring %d changes" % len(changes))
        changesTransferred = 0
        if len(changes) > 0:
            self.target.initChangeMapFile()
            self.save_previous_target_change_counter()
            self.source.progress = ReportProgress(self.source.p4, changes, self.logger, self.source.P4CLIENT)
            self.source.progress.SetSyncProgressSizeInterval(self.options.sync_progress_size_interval)
            for change in changes:
                if self.endDatetimeExceeded():
                    # Bail early
                    self.logger.info("Transfer stopped due to --end-datetime being exceeded")
                    return changesTransferred
                msg = 'Processing change: {} "{}"'.format(change['change'], change['desc'].strip())
                self.logger.info(msg)
                filerevs = self.source.getChange(change['change'])
                targetChange = self.target.replicateChange(filerevs, change, self.source.p4.port)
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
        report = "Changes transferred since %s\n%s" % (time_str,
                "\n".join(["\t".join(line) for line in lines]))
        self.logger.debug("Transfer summary report:\n%s" % report)
        self.logger.info("Sending Transfer summary report")
        self.logger.notify("Transfer summary report", report, include_output=False)
        self.save_previous_target_change_counter()
        self.target.disconnect()

    def validateClientWorkspaces(self):
        if not self.source.root == self.target.root:
            raise P4TConfigException('source and target server workspace root directories must be the same')
        src = set([m.replace("//%s/" % self.source.P4CLIENT, "") for m in self.source.clientmap.rhs()])
        targ = set([m.replace("//%s/" % self.target.P4CLIENT, "") for m in self.target.clientmap.rhs()])
        diffs = src.difference(targ)
        if diffs:
            raise P4TConfigException("Configuration failure: workspace mappings have different right hand sides: %s" % ", ".join([str(r) for r in diffs]))
        if self.source.clientspec["LineEnd"] != "unix" or self.target.clientspec["LineEnd"] != "unix":
            raise P4TConfigException("Source and target workspaces must have LineEnd set to 'unix'")
        if re.search("noclobber", self.source.clientspec["Options"]) or re.search("noclobber", self.target.clientspec["Options"]):
            raise P4TConfigException("Source and target workspaces must have 'clobber' option set")

    def setupReplicate(self):
        "Read config file and setup"
        self.readConfig()
        self.source.connect('source replicate')
        self.target.connect('target replicate')
        self.logger.debug("connected to source and target")
        sourceTargetTextComparison.setup(self.source, self.target)
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
        error_encountered = False # Flag to indicate error encountered which may require reporting
        error_notified = False
        finished = False
        num_changes = 0
        while not finished:
            try:
                self.readConfig()       # Read every time to allow user to change them
                self.logger.setReportingOptions(instance_name=self.options.instance_name,
                                mail_form_url=self.options.mail_form_url, mail_to=self.options.mail_to,
                                mail_from=self.options.mail_from, mail_server=self.options.mail_server,
                                report_interval=self.options.report_interval)
                logOnce(self.logger, self.source.options)
                logOnce(self.logger, self.target.options)
                self.source.disconnect()
                self.target.disconnect()
                num_changes = self.replicate_changes()
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
                    time.sleep(self.options.poll_interval * 60)
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
                    time.sleep(self.options.sleep_on_error_interval * 60)
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
