#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (c) 2021 Robert Cowham, Perforce Software Ltd
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
    FetchTransfer.py

DESCRIPTION:
    This python script (2.7/3.6+ compatible) will transfer Perforce changelists with all contents
    between independent servers using p4 fetch with a remote spec.

    This script transfers changes in one direction - from a source server to a target server.

    Usage:

        python3 FetchTransfer.py -h

    The script requires a config file, by default transfer.yaml,
    that provides the Perforce connection information for both servers.

    An initial example can be generated, e.g.

        FetchTransfer.py --sample-config > transfer.yaml

    For full documentation/usage, see project doc:

        https://github.com/perforce/p4transfer/blob/main/doc/FetchTransfer.adoc

"""

from __future__ import print_function, division
from os import error

import sys
import re
import stat
import pprint
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

VERSION = """$Id: 74939df934a7a660e6beff62870f65635918300b $"""


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

python3 = sys.version_info[0] >= 3
if sys.hexversion < 0x02070000 or (0x0300000 <= sys.hexversion < 0x0303000):
    sys.exit("Python 2.7 or 3.3 or newer is required to run this program.")
reFetchMoveError = re.compile("Files are missing as a result of one or more move operations")

# Although this should work with Python 3, it doesn't currently handle Windows Perforce servers
# with filenames containing charaters such as umlauts etc: åäö

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


class P4TException(Exception):
    pass


class P4TLogicException(Exception):
    pass


class P4TConfigException(P4TException):
    pass


CONFIG_FILE = 'transfer.yaml'
GENERAL_SECTION = 'general'
SOURCE_SECTION = 'source'
TARGET_SECTION = 'target'
LOGGER_NAME = "FetchTransfer"
CHANGE_MAP_DESC = "Updated change_map_file"

# This is for writing to sample config file
DEFAULT_CONFIG = yaml.load(r"""
# counter_name: Unique counter on target server to use for recording source changes processed. No spaces.
#    Name sensibly if you have multiple instances transferring into the same target p4 repository.
#    The counter value represents the last transferred change number - script will start from next change.
#    If not set, or 0 then transfer will start from first change.
counter_name: FetchTransfer_counter

# instance_name: Name of the instance of FetchTransfer - for emails etc. Spaces allowed.
instance_name: "Perforce Fetch Transfer from XYZ"

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

# max_logfile_size (Integer): Max size of file to (in bytes) after which it should be rotated
#     Typically some value such as 20MB = 20 * 1024 * 1024. Useful if transfer being run with --repeat option.
max_logfile_size: "20 * 1024 * 1024"

# change_description_format: The standard format for transferred changes.
#    Keywords prefixed with $. Use \\n for newlines. Keywords allowed:
#     $sourceDescription, $sourceChange, $sourcePort, $sourceUser
change_description_format: "$sourceDescription\\n\\nTransferred from p4://$sourcePort@$sourceChange"

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
#    Only really used by target to check in the map file if specified
workspace_root: /work/transfer

# target_remote: Name of remote spec to setup - ensure this is unique and only used by this script!
target_remote: FetchTransfer_remote

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

""")


class SourceTargetTextComparison(object):
    """Decide if source and target servers are similar OS so that text
    files can be compared by size and digest (no line ending differences)"""
    sourceVersion = None
    targetVersion = None
    sourceP4DVersion = None
    targetP4DVersion = None

    def _getServerString(self, server):
        return server.p4cmd("info", "-s")[0]["serverVersion"]

    def _getOS(self, serverString):
        parts = serverString.split("/")
        return parts[1]

    def _getP4DVersion(self, serverString):
        parts = serverString.split("/")
        return parts[2]

    def setup(self, src, targ):
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
        except TypeError:
            fh.write(contents.encode())


def ensureDirectory(directory):
    if not os.path.isdir(directory):
        os.makedirs(directory)


def makeWritable(fpath):
    "Make file writable"
    os.chmod(fpath, stat.S_IWRITE + stat.S_IREAD)


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

    def depotFileRev(self):
        "Fully specify depot file with rev number"
        return "%s#%s" % (self.depotFile, self.rev)

    def localFileRev(self):
        "Fully specify local file with rev number"
        return "%s#%s" % (self.localFile, self.rev)

    def setLocalFile(self, localFile):
        self.localFile = localFile
        localFile = localFile.replace("%40", "@")
        localFile = localFile.replace("%23", "#")
        localFile = localFile.replace("%2A", "*")
        localFile = localFile.replace("%25", "%")
        localFile = localFile.replace("/", os.sep)
        self.fixedLocalFile = localFile

    def canonicalType(self):
        "Translate between old style type and new canonical type"
        if self.type in canonicalTypes:
            return canonicalTypes[self.type]
        return self.type

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

    def createClientWorkspace(self, isSource):
        """Create or adjust client workspace for source or target
        Note that for target workspace, we don't actually create with view since that results in
        Auto-syncing as part of a fetch - which takes too long. Instead we record localmap but
        set the workspace view to allow only the change_map file to be submitted.
        """
        clientspec = self.p4.fetch_client(self.p4.client)
        logOnce(self.logger, "orig %s:%s:%s" % (self.p4id, self.p4.client, pprint.pformat(clientspec)))

        self.root = self.options.workspace_root
        clientspec._root = self.root
        clientspec["Options"] = clientspec["Options"].replace("normdir", "rmdir")
        clientspec["LineEnd"] = "unix"
        clientView = []
        dummyView = ''   # Dummy map line
        for v in self.options.views:
            exclude = ''
            lhs = v['src']
            if lhs[0] == '-':
                exclude = '-'
                lhs = lhs[1:]
            srcPath = lhs.replace('//', '')
            if isSource:
                line = "%s%s //%s/%s" % (exclude, lhs, self.p4.client, srcPath)
            else:
                line = "%s%s //%s/%s" % (exclude, v['targ'], self.p4.client, srcPath)
                if not dummyView and not exclude:
                    try:
                        depot = v['targ'][2:].split("/")[0]
                    except:
                        pass
                    dummyView = ["//%s/__dummy__ //%s/__dummy__" % (depot, self.p4.client)]
            clientView.append(line)

        if isSource:
            clientspec._view = clientView
        else:
            clientspec._view = dummyView
        self.clientmap = P4.Map(clientView)
        self.clientspec = clientspec
        self.p4.save_client(clientspec)
        logOnce(self.logger, "updated %s:%s:%s" % (self.p4id, self.p4.client, pprint.pformat(clientspec)))

        self.p4.cwd = self.root

        ctr = P4.Map('//"'+clientspec._client+'/..."   "' + clientspec._root + '/..."')
        self.localmap = P4.Map.join(self.clientmap, ctr)
        self.depotmap = self.localmap.reverse()


class P4Source(P4Base):
    "Functionality for reading from source Perforce repository"

    def __init__(self, section, options):
        super(P4Source, self).__init__(section, options, 'src')

    def missingChanges(self, counter):
        revRange = '//{client}/...@{rev},#head'.format(client=self.P4CLIENT, rev=counter + 1)
        # We can be more efficient with 2017.2 or greater servers with changes -r -m
        maxChanges = 0
        if self.options.change_batch_size:
            maxChanges = self.options.change_batch_size
        if self.options.maximum and self.options.maximum < maxChanges:
            maxChanges = self.options.maximum
        args = ['changes', '-l', '-r']
        if maxChanges > 0:
            args.extend(['-m', maxChanges])
        args.append(revRange)
        self.logger.debug('reading changes: %s' % args)
        changes = self.p4cmd(args)
        self.logger.debug('processing %d changes' % len(changes))
        return changes

    def getChange(self, changeNum):
        """Expects change number as a string, and returns list of filerevs"""
        change = self.p4cmd('describe', '-s', changeNum)[0]
        fileRevs = []
        excludedFiles = []
        for (n, rev) in enumerate(change['rev']):
            localFile = self.localmap.translate(change['depotFile'][n])
            if localFile and len(localFile) > 0:
                chRev = ChangeRevision(rev, change, n)
                chRev.setLocalFile(localFile)
                fileRevs.append(chRev)
            else:
                excludedFiles.append(change['depotFile'][n])
        if excludedFiles:
            self.logger.debug('excluded: %s' % excludedFiles)
        return fileRevs


class P4Target(P4Base):
    "Functionality for transferring changes to target Perforce repository"

    def __init__(self, section, options, source):
        super(P4Target, self).__init__(section, options, 'targ')
        self.source = source
        self.filesToIgnore = []

    def createRemoteSpec(self, source):
        remoteSpec = self.p4.fetch_remote(self.options.target_remote)
        logOnce(self.logger, "orig %s:%s:%s" % (self.p4id, self.p4.client, pprint.pformat(remoteSpec)))

        remoteSpec["Address"] = source.p4.port
        remoteSpec["DepotMap"] = []
        exclude = ''
        for v in self.options.views:
            rhs = v['src']
            if rhs[0] == '-':
                exclude = '-'
                rhs = rhs[1:]
            line = "%s%s %s" % (exclude, v['targ'], rhs)
            remoteSpec["DepotMap"].append(line)

        self.remoteSpec = remoteSpec
        self.p4.save_remote(remoteSpec)
        logOnce(self.logger, "updated %s:%s:%s" % (self.p4id, self.options.target_remote, pprint.pformat(remoteSpec)))

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

    def fetchWithFlags(self, chgNo, flags):
        "fetch which can be repeated with flags where necessary"
        resultStr = ""
        resultDict = {}
        try:
            cmd = ["fetch", "-v", "-r", self.options.target_remote]
            cmd.extend(flags)
            cmd.append("//...@%s,%s" % (chgNo, chgNo))
            output = self.p4cmd(cmd)
            if output:
                if isinstance(output[0], dict):
                    resultDict = output[0]
                else:
                    resultStr = output[0]
        except P4.P4Exception as e:
            resultStr += "\n".join(self.p4.errors)
            resultStr += "\n".join(e.errors)
        resultStr += "\n".join(self.p4.warnings)
        if resultStr:
            self.logger.debug(resultStr)
        # Weird side effect - some errors can disconnect!!
        if not self.p4.connected():
            self.p4.connect()
        return (resultDict, resultStr)

    def doFetch(self, chgNo):
        "Perform integrate"
        outputDict = {}
        flags = []
        while 1 == 1:
            outputDict, outputStr = self.fetchWithFlags(chgNo, flags)
            if reFetchMoveError.search(outputStr) and "-I" not in flags:
                flags.append("-I")
            else:
                break
        return outputDict

    def replicateChange(self, change, srcFileRevs):
        """This is the heart of it all. Replicate a single change"""

        self.filesToIgnore = []
        result = self.doFetch(change['change'])
        newChangeId = 0
        if result:
            if 'renamedChange' in result:
                newChangeId = result['renamedChange']
        if newChangeId:
            self.logger.info("source = {} : target  = {}".format(change['change'], newChangeId))
            description = self.formatChangeDescription(
                sourceDescription=change['desc'],
                sourceChange=change['change'], sourcePort=self.source.p4.port,
                sourceUser=change['user'])
            self.updateChange(newChangeId=newChangeId, description=description)
        else:
            self.logger.error("failed to replicate change {}".format(change['change']))
        self.validateSubmittedChange(srcFileRevs, newChangeId)
        return newChangeId

    def validateSubmittedChange(self, srcFileRevs, newChangeId):
        "Check against what was passed in"
        targFileRevs = []
        if newChangeId:
            change = self.p4cmd('describe', '-s', newChangeId)[0]
            for (n, rev) in enumerate(change['rev']):
                localFile = self.localmap.translate(change['depotFile'][n])
                if localFile and len(localFile) > 0:
                    chRev = ChangeRevision(rev, change, n)
                    chRev.setLocalFile(localFile)
                    targFileRevs.append(chRev)
        cc = ChangelistComparer(self.logger)
        result = cc.listsEqual(srcFileRevs, targFileRevs, self.filesToIgnore)
        if not result[0]:
            raise P4TLogicException(result[1])

    def updateChange(self, newChangeId, description):
        # need to update the user and time stamp - but only if a superuser
        if not self.options.superuser == "y":
            return
        newChange = self.p4.fetch_change(newChangeId)
        newChange._description = description
        self.p4.save_change(newChange, '-f')

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


class FetchTransfer(object):
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
        parser.add_argument('-r', '--repeat', action='store_true',
                            help="Repeat transfer in a loop - for continuous transfer as background task")
        parser.add_argument('-s', '--stoponerror', action='store_true', help="Stop on any error even if --repeat has been specified")
        parser.add_argument('--sample-config', action='store_true', help="Print an example config file and exit")
        parser.add_argument('-i', '--ignore-integrations', action='store_true', help="Treat integrations as adds and edits")
        parser.add_argument('--end-datetime', type=valid_datetime_type, default=None,
                            help="Time to stop transfers, format: 'YYYY/MM/DD HH:mm' - useful"
                            " for automation runs during quiet periods e.g. run overnight but stop first thing in the morning")
        self.options = parser.parse_args(list(args))

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
        self.options.max_logfile_size = self.getIntOption(GENERAL_SECTION, "max_logfile_size", 20 * 1024 * 1024)
        self.options.change_description_format = self.getOption(
            GENERAL_SECTION, "change_description_format",
            "$sourceDescription\n\nTransferred from p4://$sourcePort@$sourceChange")
        self.options.change_map_file = self.getOption(GENERAL_SECTION, "change_map_file", "")
        self.options.superuser = self.getOption(GENERAL_SECTION, "superuser", "y")
        self.options.views = self.getOption(GENERAL_SECTION, "views")
        self.options.target_remote = self.getOption(GENERAL_SECTION, "target_remote")
        self.options.workspace_root = self.getOption(GENERAL_SECTION, "workspace_root")
        self.options.ignore_files = self.getOption(GENERAL_SECTION, "ignore_files")
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
        self.source.createClientWorkspace(isSource=True)
        self.target.createClientWorkspace(isSource=False)
        changes = self.source.missingChanges(self.target.getCounter())
        if self.options.notransfer:
            self.logger.info("Would transfer %d changes - stopping due to --notransfer" % len(changes))
            return 0
        self.logger.info("Transferring %d changes" % len(changes))
        changesTransferred = 0
        if len(changes) > 0:
            self.target.initChangeMapFile()
            self.save_previous_target_change_counter()
            self.checkRotateLogFile()
            self.revertOpenedFiles()
            for change in changes:
                if self.endDatetimeExceeded():
                    # Bail early
                    self.logger.info("Transfer stopped due to --end-datetime being exceeded")
                    return changesTransferred
                msg = 'Processing change: {} "{}"'.format(
                            change['change'], change['desc'].strip())
                self.logger.info(msg)
                srcFileRevs = self.source.getChange(change['change'])
                targetChange = self.target.replicateChange(change, srcFileRevs)
                self.target.setCounter(change['change'])
                self.target.updateChangeMap(self.source.p4.port, change['change'], targetChange)
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

    def validateConfig(self):
        "Performs appropriate validation of config values - primarily streams"
        pass

    def setupReplicate(self):
        "Read config file and setup - raises exceptions if invalid"
        self.readConfig()
        self.source.connect('source replicate')
        self.target.connect('target replicate')
        self.validateConfig()
        self.logger.debug("connected to source and target")
        self.source.createClientWorkspace(isSource=True)
        self.target.createClientWorkspace(isSource=False)
        self.target.createRemoteSpec(self.source)
        # sourceTargetTextComparison.setup(self.source, self.target)

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
        prog = FetchTransfer(*sys.argv[1:])
        result = prog.replicate()
    except Exception as e:
        print(str(e))
        result = 1
    sys.exit(result)
