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
    CopySnapshot.py

DESCRIPTION:
    This python script (3.6+ compatible) to copy a snapshot between two P4 servers.
    It is a companion script to P4Transfer.py and reads the same source/target config file.

    Usage:

        python3 CopySnapshot.py -h

    The script requires a config file as for P4Transfer.py
    that provides the Perforce connection information for both servers.

    For full documentation/usage, see project doc:

        https://github.com/perforce/p4transfer/blob/main/doc/P4Transfer.adoc

"""

import P4
import os
import re
import sys
import argparse
import textwrap
from ruamel.yaml import YAML
yaml = YAML()


# This is updated based on the value in the config file - used in comparisons below
caseSensitive = True


class FileRev:
    def __init__(self, f) -> None:
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
        self.localFile = ""
        self.fixedLocalFile = ""

    def setLocalFile(self, localFile):
        if not localFile:
            return
        self.localFile = localFile
        localFile = localFile.replace("%40", "@")
        localFile = localFile.replace("%23", "#")
        localFile = localFile.replace("%2A", "*")
        localFile = localFile.replace("%25", "%")
        localFile = localFile.replace("/", os.sep)
        self.fixedLocalFile = localFile

    def __repr__(self):
        return 'depotFile={depotfile} rev={rev} action={action} type={type} size={size} digest={digest}' .format(
            rev=self.rev,
            action=self.action,
            type=self.type,
            size=self.fileSize,
            digest=self.digest,
            depotfile=self.depotFile,
        )

    def __eq__(self, other):
        "For comparisons between source and target after transfer"
        if caseSensitive:
            if self.localFile != other.localFile:   # Check filename
                return False
        else:
            if self.localFile.lower() != other.localFile.lower():
                return False
        if "delete" in self.action and "delete" in other.action:
            return True
        if (self.fileSize, self.digest) != (other.fileSize, other.digest):
            return False
        return True


class CaseFixer:
    "Class to fix case names - for now hard coded fixes"
    
    def fixCase(self, localPath):
        if os.path.exists(localPath):
            return localPath
        localPath = localPath.replace("/engine/", "/Engine/")
        localPath = localPath.replace("/samples/", "/Samples/")
        localPath = localPath.replace("/QAGAME/", "/QAGame/")
        localPath = localPath.replace("/qagame/", "/QAGame/")
        localPath = localPath.replace("/engineTest/", "/EngineTest/")
        if not os.path.exists(localPath):
            raise Exception("Failed to fix: %s" % localPath)
        return localPath


class CopySnapshot():
    
    def __init__(self) -> None:
        desc = textwrap.dedent(__doc__)
        parser = argparse.ArgumentParser(
            description=desc,
            formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog="Copyright (C) 2021 Robert Cowham, Perforce Software Ltd"
        )

        parser.add_argument('-c', '--config', help="Config file as used by P4Transfer - to read source/target info")
        parser.add_argument('-w', '--workspace', help="Source workspace to use (otherwise will use the one in the config file)")
        parser.add_argument('-s', '--source', help="Perforce path for source repo, e.g. //depot/src/...@52342")
        parser.add_argument('-t', '--target', help="Perforce path for target repo, e.g. //depot/targ/...@123")
        self.options = parser.parse_args()

        if not self.options.config or not os.path.exists(self.options.config):
            parser.print_help()
            print("\nNo config file was specified or it doesn't exist!\nSee help above.")
            sys.exit(1)
        with open(self.options.config) as f:
            self.config = yaml.load(f)

        self.srcp4 = P4.P4()
        self.srcp4.port = self.config['source']['p4port']
        self.srcp4.user = self.config['source']['p4user']
        self.srcp4.client = self.config['source']['p4client']
        if self.options.workspace:
            self.srcp4.client = self.options.workspace
        self.srcp4.connect()
        self.targp4 = P4.P4()
        self.targp4.port = self.config['target']['p4port']
        self.targp4.user = self.config['target']['p4user']
        self.targp4.client = self.config['target']['p4client']
        self.targp4.connect()
        global caseSensitive
        caseSensitive = self.config['case_sensitive']
        srcClient = self.srcp4.fetch_client()
        self.clientmap = P4.Map(srcClient._view)
        ctr = P4.Map('//"' + srcClient._client + '/..."   "' + srcClient._root + '/..."')
        self.mapToLocal = P4.Map.join(self.clientmap, ctr)

    def getFiles(self, fstat):
        result = {}
        for f in fstat:
            fname = f['depotFile']
            if not caseSensitive:
                fname = fname.lower()
            rev = FileRev(f)
            localFile = self.mapToLocal.translate(rev.depotFile)
            if localFile:
                rev.setLocalFile(localFile)
                result[fname] = rev
            else:
                print("Ignoring source: %s" % rev.depotFile)
        return result
    
    def run(self):
        srcFiles = {}
        print("Collecting source files")
        srcFstat = self.srcp4.run_fstat("-Ol", self.options.source)
        srcFiles = self.getFiles(srcFstat)    
        print("Found source files: %d" % len(srcFiles))
        print("Checking sync of source files")
        self.srcp4.run('sync', self.options.source)
        # Create a target change to open files in
        chg = self.targp4.fetch_change()
        chg['Description'] = "Import of snapshot %s" % self.options.source
        output = self.targp4.save_change(chg)[0]
        m = re.search("Change ([0-9]+) created", output)
        if not m:
            raise Exception("Failed to create changelist")
        chgno = m.group(1)
        fixer = CaseFixer()
        for _, v in srcFiles.items():
            if 'delete' not in v.action:
                localPath = fixer.fixCase(v.fixedLocalFile)
                self.targp4.run('add', '-c', chgno, '-ft', v.type, localPath)
        print("All files opened in changelist: %s" % chgno)
        print("Recommend running: nohup p4 submit -c %s > sub.out &" % chgno)
        print("Then monitor the output for completion.")


if __name__ == '__main__':
    obj = CopySnapshot()
    obj.run()
