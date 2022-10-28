# -*- encoding: UTF8 -*-
# Tests for the CompareRepos.py module.

from __future__ import print_function

import sys
import time
import P4
import subprocess
import inspect
import unittest
import os
import shutil
import stat
import argparse
from ruamel.yaml import YAML

# Bring in module to be tested
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
import logutils         # noqa: E402
import CompareRepos       # noqa: E402

yaml = YAML()

python3 = sys.version_info[0] >= 3
if sys.hexversion < 0x02070000 or (0x0300000 < sys.hexversion < 0x0303000):
    sys.exit("Python 2.7 or 3.3 or newer is required to run this program.")

if python3:
    from io import StringIO
else:
    from StringIO import StringIO

P4D = "p4d"     # This can be overridden via command line stuff
P4USER = "testuser"
P4CLIENT = "test_ws"
TEST_ROOT = '_testrun_transfer'
TRANSFER_CLIENT = "transfer"
TRANSFER_CONFIG = "transfer.yaml"

TEST_COUNTER_NAME = "CompareRepos"
LOGGER_NAME = "TestCompareRepos"

saved_stdoutput = StringIO()
test_logger = None


def onRmTreeError(function, path, exc_info):
    os.chmod(path, stat.S_IWRITE)
    os.remove(path)


def ensureDirectory(directory):
    if not os.path.isdir(directory):
        os.makedirs(directory)


def localDirectory(root, *dirs):
    "Create and ensure it exists"
    dir_path = os.path.join(root, *dirs)
    ensureDirectory(dir_path)
    return dir_path


def create_file(file_name, contents):
    "Create file with specified contents"
    ensureDirectory(os.path.dirname(file_name))
    if python3:
        contents = bytes(contents.encode())
    with open(file_name, 'wb') as f:
        f.write(contents)


def append_to_file(file_name, contents):
    "Append contents to file"
    if python3:
        contents = bytes(contents.encode())
    with open(file_name, 'ab+') as f:
        f.write(contents)


def escapeName(fname):
    fname = fname.replace("%", "%25")
    fname = fname.replace("#", "%23")
    fname = fname.replace("@", "%40")
    fname = fname.replace("*", "%2A")
    return fname


def getP4ConfigFilename():
    "Returns os specific filename"
    if 'P4CONFIG' in os.environ:
        return os.environ['P4CONFIG']
    if os.name == "nt":
        return "p4config.txt"
    return ".p4config"


class P4Server:
    def __init__(self, root, logger):
        self.root = root
        self.logger = logger
        self.server_root = os.path.join(root, "server")
        self.client_root = os.path.join(root, "client")

        ensureDirectory(self.root)
        ensureDirectory(self.server_root)
        ensureDirectory(self.client_root)

        self.p4d = P4D
        self.port = "rsh:%s -r \"%s\" -L log -i" % (self.p4d, self.server_root)
        self.p4 = P4.P4()
        self.p4.port = self.port
        self.p4.user = P4USER
        self.p4.client = P4CLIENT
        self.p4.connect()

        self.p4cmd('depots')  # triggers creation of the user

        self.p4.disconnect()  # required to pick up the configure changes
        self.p4.connect()

        self.client_name = P4CLIENT
        client = self.p4.fetch_client(self.client_name)
        client._root = self.client_root
        client._lineend = 'unix'
        self.p4.save_client(client)

    def shutDown(self):
        if self.p4.connected():
            self.p4.disconnect()

    def createTransferClient(self, name, root):
        pass

    def run_cmd(self, cmd, dir=".", get_output=True, timeout=35, stop_on_error=True):
        "Run cmd logging input and output"
        output = ""
        try:
            self.logger.debug("Running: %s" % cmd)
            if get_output:
                p = subprocess.Popen(cmd, cwd=dir, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True, shell=True)
                if python3:
                    output, _ = p.communicate(timeout=timeout)
                else:
                    output, _ = p.communicate()
                # rc = p.returncode
                self.logger.debug("Output:\n%s" % output)
            else:
                result = subprocess.check_call(cmd, stderr=subprocess.STDOUT, shell=True, timeout=timeout)
                self.logger.debug('Result: %d' % result)
        except subprocess.CalledProcessError as e:
            self.logger.debug("Output: %s" % e.output)
            if stop_on_error:
                msg = 'Failed run_cmd: %d %s' % (e.returncode, str(e))
                self.logger.debug(msg)
                raise e
        except Exception as e:
            self.logger.debug("Output: %s" % output)
            if stop_on_error:
                msg = 'Failed run_cmd: %s' % str(e)
                self.logger.debug(msg)
                raise e
        return output

    def enableUnicode(self):
        cmd = '%s -r "%s" -L log -vserver=3 -xi' % (self.p4d, self.server_root)
        output = self.run_cmd(cmd, dir=self.server_root, get_output=True, stop_on_error=True)
        self.logger.debug(output)

    def getCounter(self):
        "Returns value of counter as integer"
        result = self.p4.run('counter', TEST_COUNTER_NAME)
        if result and 'counter' in result[0]:
            return int(result[0]['value'])
        return 0

    def p4cmd(self, *args):
        "Execute p4 cmd while logging arguments and results"
        if not self.logger:
            self.logger = logutils.getLogger(LOGGER_NAME)
        self.logger.debug('testp4:', args)
        output = self.p4.run(args)
        self.logger.debug('testp4r:', output)
        return output


class TestCompareRepos(unittest.TestCase):

    def __init__(self, methodName='runTest'):
        global saved_stdoutput, test_logger
        saved_stdoutput.truncate(0)
        if test_logger is None:
            test_logger = logutils.getLogger(LOGGER_NAME, stream=saved_stdoutput)
        else:
            logutils.resetLogger(LOGGER_NAME)
        self.logger = test_logger
        super(TestCompareRepos, self).__init__(methodName=methodName)

    def assertRegex(self, *args, **kwargs):
        if python3:
            return super(TestCompareRepos, self).assertRegex(*args, **kwargs)
        else:
            return super(TestCompareRepos, self).assertRegexpMatches(*args, **kwargs)

    def assertContentsEqual(self, expected, content):
        if python3:
            content = content.decode()
        self.assertEqual(expected, content)

    def setUp(self):
        self.setDirectories()

    def tearDown(self):
        self.source.shutDown()
        self.target.shutDown()
        time.sleep(0.1)
        # self.cleanupTestTree()

    def setDirectories(self):
        self.startdir = os.getcwd()
        self.transfer_root = os.path.join(self.startdir, TEST_ROOT)
        self.cleanupTestTree()

        ensureDirectory(self.transfer_root)

        self.source = P4Server(os.path.join(self.transfer_root, 'source'), self.logger)
        self.target = P4Server(os.path.join(self.transfer_root, 'target'), self.logger)

        self.transfer_client_root = localDirectory(self.transfer_root, 'transfer_client')
        self.writeP4Config()

    def writeP4Config(self):
        "Write appropriate files - useful for occasional manual debugging"
        p4config_filename = getP4ConfigFilename()
        srcP4Config = os.path.join(self.transfer_root, 'source', p4config_filename)
        targP4Config = os.path.join(self.transfer_root, 'target', p4config_filename)
        transferP4Config = os.path.join(self.transfer_client_root, p4config_filename)
        with open(srcP4Config, "w") as fh:
            fh.write('P4PORT=%s\n' % self.source.port)
            fh.write('P4USER=%s\n' % self.source.p4.user)
            fh.write('P4CLIENT=%s\n' % self.source.p4.client)
        with open(targP4Config, "w") as fh:
            fh.write('P4PORT=%s\n' % self.target.port)
            fh.write('P4USER=%s\n' % self.target.p4.user)
            fh.write('P4CLIENT=%s\n' % self.target.p4.client)
        with open(transferP4Config, "w") as fh:
            fh.write('P4PORT=%s\n' % self.target.port)
            fh.write('P4USER=%s\n' % self.target.p4.user)
            fh.write('P4CLIENT=%s\n' % TRANSFER_CLIENT)

    def cleanupTestTree(self):
        os.chdir(self.startdir)
        if os.path.isdir(self.transfer_root):
            shutil.rmtree(self.transfer_root, False, onRmTreeError)

    def getDefaultOptions(self):
        config = {}
        config['transfer_client'] = TRANSFER_CLIENT
        config['workspace_root'] = self.transfer_client_root
        config['views'] = [{'src': '//depot/...',
                            'targ': '//depot/...'}]
        return config

    def setupCompare(self):
        """Creates a config file with default mappings"""
        msg = "Test: %s ======================" % inspect.stack()[1][3]
        self.logger.debug(msg)
        config = self.getDefaultOptions()
        self.createConfigFile(options=config)
        client = self.source.p4.fetch_client(config['transfer_client'])
        client._root = config['workspace_root']
        client._view = ['//depot/... //%s/depot/...' % (config['transfer_client'])]
        self.source.p4.save_client(client)
        client = self.target.p4.fetch_client(config['transfer_client'])
        client._root = config['workspace_root']
        client._view = ['//depot/... //%s/depot/...' % (config['transfer_client'])]
        self.target.p4.save_client(client)
        self.transferp4 = P4.P4()
        self.transferp4.port = self.target.p4.port
        self.transferp4.user = self.target.p4.user
        self.transferp4.client = config['transfer_client']
        self.transferp4.connect()

    def createConfigFile(self, srcOptions=None, targOptions=None, options=None):
        "Creates config file with extras if appropriate"
        if options is None:
            options = {}
        if srcOptions is None:
            srcOptions = {}
        if targOptions is None:
            targOptions = {}

        config = {}
        config['case_sensitive'] = 'False'
        config['source'] = {}
        config['source']['p4port'] = self.source.port
        config['source']['p4user'] = P4USER
        config['source']['p4client'] = TRANSFER_CLIENT
        for opt in srcOptions.keys():
            config['source'][opt] = srcOptions[opt]

        config['target'] = {}
        config['target']['p4port'] = self.target.port
        config['target']['p4user'] = P4USER
        config['target']['p4client'] = TRANSFER_CLIENT
        for opt in targOptions.keys():
            config['target'][opt] = targOptions[opt]

        config['logfile'] = os.path.join(self.transfer_root, 'temp', 'test.log')
        if not os.path.exists(os.path.join(self.transfer_root, 'temp')):
            os.mkdir(os.path.join(self.transfer_root, 'temp'))
        config['counter_name'] = TEST_COUNTER_NAME

        for opt in options.keys():
            config[opt] = options[opt]

        # write the config file
        self.transfer_cfg = os.path.join(self.transfer_root, TRANSFER_CONFIG)
        with open(self.transfer_cfg, 'w') as f:
            yaml.dump(config, f)

    def run_CompareRepos(self, *args):
        self.logger.debug("-----------------------------Starting CompareRepos")
        base_args = ['-c', self.transfer_cfg]
        if args:
            base_args.extend(args)
        obj = CompareRepos.CompareRepos(*base_args)
        result = obj.run()
        return result

    def testAddNoHave(self):
        "Basic file add where no files already synced"
        self.setupCompare()

        inside = localDirectory(self.source.client_root, "inside")
        inside_file1 = os.path.join(inside, "inside_file1")
        create_file(inside_file1, 'Test content')

        self.source.p4cmd('add', inside_file1)
        self.source.p4cmd('submit', '-d', 'inside_file1 added')

        self.run_CompareRepos('-s', '//depot/...@3', '--fix')
        self.transferp4.run('submit', '-d', "Target")

        changes = self.target.p4cmd('changes')
        self.assertEqual(1, len(changes))

        files = self.target.p4cmd('files', '//depot/...')
        self.assertEqual(1, len(files))
        self.assertEqual('//depot/inside/inside_file1', files[0]['depotFile'])

    def testAddWithHave(self):
        "Basic file add where source files already synced"
        self.setupCompare()

        inside = localDirectory(self.source.client_root, "inside")
        inside_file1 = os.path.join(inside, "inside_file1")
        create_file(inside_file1, 'Test content')

        self.source.p4cmd('add', inside_file1)
        self.source.p4cmd('submit', '-d', 'inside_file1 added')

        p4 = self.source.p4
        p4.client = TRANSFER_CLIENT
        p4.run_sync()

        self.run_CompareRepos('-s', '//depot/...@3', '--fix')
        self.transferp4.run('submit', '-d', "Target")

        changes = self.target.p4cmd('changes')
        self.assertEqual(1, len(changes))

        files = self.target.p4cmd('files', '//depot/...')
        self.assertEqual(1, len(files))
        self.assertEqual('//depot/inside/inside_file1', files[0]['depotFile'])

    def testAddWithHaveWildcard(self):
        "Basic file add where source files already synced"
        self.setupCompare()

        inside = localDirectory(self.source.client_root, "inside")
        inside_file1 = os.path.join(inside, "inside_file1@png")
        create_file(inside_file1, 'Test content')

        self.source.p4cmd('add', '-f', inside_file1)
        self.source.p4cmd('submit', '-d', 'inside_file1 added')

        p4 = self.source.p4
        p4.client = TRANSFER_CLIENT
        p4.run_sync()

        self.run_CompareRepos('-s', '//depot/...@3', '--fix')
        self.transferp4.run('submit', '-d', "Target")

        changes = self.target.p4cmd('changes')
        self.assertEqual(1, len(changes))

        files = self.target.p4cmd('files', '//depot/...')
        self.assertEqual(1, len(files))
        self.assertEqual('//depot/inside/inside_file1%40png', files[0]['depotFile'])

        # Now do edit too
        p4.client = P4CLIENT
        self.source.p4cmd('edit', escapeName(inside_file1))
        create_file(inside_file1, 'Test content2')
        self.source.p4cmd('submit', '-d', 'inside_file1 edited')

        p4.client = TRANSFER_CLIENT
        p4.run_sync()

        self.run_CompareRepos('-s', '//depot/...@4', '--fix')
        self.transferp4.run('submit', '-d', "Target2")

        changes = self.target.p4cmd('changes')
        self.assertEqual(2, len(changes))

        files = self.target.p4cmd('files', '//depot/...')
        self.assertEqual(1, len(files))
        self.assertEqual('//depot/inside/inside_file1%40png', files[0]['depotFile'])


        # And delete
        p4.client = P4CLIENT
        self.source.p4cmd('delete', escapeName(inside_file1))
        self.source.p4cmd('submit', '-d', 'inside_file1 deleted')

        p4.client = TRANSFER_CLIENT

        self.run_CompareRepos('-s', '//depot/...@5', '--fix')
        self.transferp4.run('submit', '-d', "Target3")

        changes = self.target.p4cmd('changes')
        self.assertEqual(3, len(changes))

        files = self.target.p4cmd('files', '//depot/...')
        self.assertEqual(1, len(files))
        self.assertEqual('//depot/inside/inside_file1%40png', files[0]['depotFile'])


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--p4d', default=P4D)
    parser.add_argument('unittest_args', nargs='*')

    args = parser.parse_args()
    if args.p4d != P4D:
        P4D = args.p4d

    # Now set the sys.argv to the unittest_args (leaving sys.argv[0] alone)
    unit_argv = [sys.argv[0]] + args.unittest_args
    unittest.main(argv=unit_argv)
