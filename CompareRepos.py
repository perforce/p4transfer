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
    CompareRepos.py

DESCRIPTION:
    This python script (3.6+ compatible) will compare changelists in 2 repos
    It is a companion script to P4Transfer.py and reads the same source/target config file.

    Usage:

        python3 CompareRepos.py -h

    The script requires a config file as for P4Transfer.py
    The config file provides the Perforce connection information for both servers, 
    and source/target client workspace names.

    For full documentation/usage, see project doc:

        https://github.com/perforce/p4transfer/blob/main/doc/P4Transfer.adoc

"""

import P4
import os
import platform
import re
import argparse
import textwrap
from ruamel.yaml import YAML
yaml = YAML()


# This is updated based on the value in the config file - used in comparisons below
caseSensitive = (platform.system() == "Linux")
alreadyEscaped = re.compile(r"%25|%23|%40|%2A")


def escapeWildcards(fname):
    m = alreadyEscaped.findall(fname)
    if not m:
        fname = fname.replace("%", "%25")
        fname = fname.replace("#", "%23")
        fname = fname.replace("@", "%40")
        fname = fname.replace("*", "%2A")
    return fname


class FileRev:
    def __init__(self, f):
        self.depotFile = f['depotFile']
        self.action = f['headAction']
        self.digest = ""
        self.fileSize = 0
        if 'digest' in f:
            self.digest = f['digest']
        if 'fileSize' in f:
            self.fileSize = f['fileSize']
        self.rev = f['headRev']
        self.change = f['headChange']
        self.type = f['headType']

    def __repr__(self):
        return 'depotFile={depotfile} rev={rev} action={action} type={type} size={size} digest={digest}' .format(
            rev=self.rev,
            action=self.action,
            type=self.type,
            size=self.fileSize,
            digest=self.digest,
            depotfile=self.depotFile,
        )


class CompareRepos():

    def __init__(self, *args):
        desc = textwrap.dedent(__doc__)
        parser = argparse.ArgumentParser(
            description=desc,
            formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog="Copyright (C) 2021-22 Robert Cowham, Perforce Software Ltd"
        )

        parser.add_argument('-c', '--config', help="Config file as used by P4Transfer - to read source/target info")
        parser.add_argument('-s', '--source', help="Perforce path for source repo, e.g. //depot/src/...@52342")
        parser.add_argument('-t', '--target', help="Optional: Perforce path for target repo, e.g. //depot/targ/...@123 " +
                            "[or without rev for #head]. " +
                            "If not specified then assumes --source value with revision specifier removed")
        parser.add_argument('-f', '--fix', action='store_true', help="Fix problems by opening files for required action on target to make src/target the same")
        if list(args):
            self.options = parser.parse_args(list(args))
        else:
            self.options = parser.parse_args()

        if not os.path.exists(self.options.config):
            raise Exception("No config file was specified!")
        with open(self.options.config) as f:
            self.config = yaml.load(f)

        if not self.options.target:
            if "@" in self.options.source:
                parts = self.options.source.split("@")
                if len(parts) > 1:
                    self.options.target = parts[0]
        if not self.options.target:
            raise Exception("Please specify --target or a changelist for source, e.g. --source //some/path/...@12345")
        self.srcp4 = P4.P4()
        self.srcp4.port = self.config['source']['p4port']
        self.srcp4.user = self.config['source']['p4user']
        if self.options.fix:
            self.srcp4.client = self.config['source']['p4client']
        self.srcp4.connect()
        self.targp4 = P4.P4()
        self.targp4.port = self.config['target']['p4port']
        self.targp4.user = self.config['target']['p4user']
        if self.options.fix:
            self.targp4.client = self.config['target']['p4client']
        self.targp4.connect()
        if platform.system() != "Linux": # Allow to be overwritten
            global caseSensitive
            caseSensitive = self.config['case_sensitive']

    def getFiles(self, fstat): # Returns 2 lists: depot files and local files
        result = {}
        srcLocalFiles = {}
        for f in fstat:
            fname = f['depotFile']
            if not caseSensitive:
                fname = fname.lower()
            result[fname] = FileRev(f)
            if 'clientFile' in f:
                srcLocalFiles[fname] = f['clientFile']
        return result, srcLocalFiles

    def run(self):
        srcFiles = {}
        targFiles = {}
        print("Collecting source files: %s" % self.options.source)
        srcFstat = self.srcp4.run_fstat("-Ol", self.options.source)
        # Important to record the file names in local syntax for when source server is case-insensitive and
        # the local OS is case sensitive!
        # Then the source depotFile and localFile can differ in case. However when synced, p4 will
        # at least be case consistent with previous files/directories in the tree.
        srcLocalHaveFiles = {}
        if self.options.fix:
            srcPath = ""
            if "@" in self.options.source:
                parts = self.options.source.split("@")
                if len(parts) > 1:
                    srcPath = parts[0]
            haveList = []
            with self.srcp4.at_exception_level(P4.P4.RAISE_NONE):
                haveList = self.srcp4.run('have', srcPath)
            for f in haveList:
                if caseSensitive:
                    k = f['depotFile']
                else:
                    k = f['depotFile'].lower()
                srcLocalHaveFiles[k] = f['path']
        print("Collecting target files: %s" % self.options.target)
        targFstat = []
        with self.targp4.at_exception_level(P4.P4.RAISE_NONE):
            targFstat = self.targp4.run_fstat("-Ol", self.options.target)
        srcFiles, srcLocalFiles = self.getFiles(srcFstat)
        targFiles, targLocalFiles = self.getFiles(targFstat)
        missing = []
        deleted = []
        extras = []
        different = []
        for k, v in srcFiles.items():
            if 'delete' not in v.action:
                if k not in targFiles:
                    missing.append(k)
                    if self.options.fix:
                        if caseSensitive:
                            dfile = k
                        else:
                            dfile = k.lower()
                        if dfile not in srcLocalHaveFiles:  # Otherwise we assume already manually synced
                            if dfile in srcLocalFiles:
                                nfile = escapeWildcards(srcLocalFiles[k])
                            else:
                                nfile = escapeWildcards(dfile)
                            print("src: %s" % self.srcp4.run_sync('-f', "%s#%s" % (nfile, v.rev)))
                        print(self.targp4.run_add('-ft', v.type, srcLocalFiles[k]))
                if k in targFiles and 'delete' in targFiles[k].action:
                    deleted.append((k, targFiles[k].change))
                    if self.options.fix:
                        print(self.srcp4.run_sync('-f', "%s#%s" % (escapeWildcards(srcLocalFiles[k]), v.rev)))
                        print(self.targp4.run_add('-ft', v.type, srcLocalFiles[k]))
            if 'delete' not in v.action:
                if k in targFiles and 'delete' not in targFiles[k].action and v.digest != targFiles[k].digest:
                    different.append((k, v, targFiles[k]))
                    if self.options.fix:
                        with self.targp4.at_exception_level(P4.P4.RAISE_NONE):
                            print(self.targp4.run_sync("-k", targLocalFiles[k]))
                        print(self.srcp4.run_sync('-f', "%s#%s" % (escapeWildcards(srcLocalFiles[k]), v.rev)))
                        print(self.targp4.run_edit('-t', v.type, escapeWildcards(srcLocalFiles[k])))
        for k, v in targFiles.items():
            if 'delete' not in v.action:
                if k not in srcFiles or (k in srcFiles and 'delete' in srcFiles[k].action):
                    extras.append(k)
                    if self.options.fix:
                        with self.targp4.at_exception_level(P4.P4.RAISE_NONE):
                            print(self.targp4.run_sync('-k', escapeWildcards(targLocalFiles[k])))
                        print(self.targp4.run_delete(escapeWildcards(targLocalFiles[k])))
        missingCount = deletedCount = extrasCount = differentCount = 0
        if missing:
            print("missing: %s" % "\nmissing: ".join(missing))
            missingCount = len(missing)
        else:
            print("No-missing")
        if deleted:
            print("deleted: %s" % "\ndeleted: ".join(["%s:%s" % (x[1], x[0]) for x in deleted]))
            deletedCount = len(deleted)
        else:
            print("No-deleted")
        if extras:
            print("extra: %s" % "\nextra: ".join(extras))
            extrasCount = len(extras)
        else:
            print("No-extras")
        if different:
            print("different: %s" % "\ndifferent: ".join([x[0] for x in different]))
            differentCount = len(different)
        else:
            print("No-different")
        print("""
Total missing:   %d
Total deleted:   %d
Total extras:    %d
Total different: %d
Sum Total:       %d
""" % (missingCount, deletedCount, extrasCount, differentCount, missingCount + deletedCount + extrasCount + differentCount))

if __name__ == '__main__':
    obj = CompareRepos()
    obj.run()
