
# depotFile //UE5/Release-5.0/.editorconfig       headAction branch       fileSize 542    digest 482C321884873DA1C9E234A1BFA13E40
# depotFile //UE5/Release-5.0/Engine/Binaries/DotNET/AgentInterface.dll   headAction branch       fileSize 12288  digest D93D0F36DBD1F650602681DE5A64E5CA
# depotFile //UE5/Release-5.0/Engine/Binaries/DotNET/AgentInterface.pdb   headAction branch       fileSize 22016  digest 83884854293716F9E7A2401364EEB04E
# depotFile //UE5/Release-5.0/Engine/Binaries/DotNET/CsvTools/CSVCollate.exe      headAction branch       fileSize 10240  digest 362643C3AC2C67637E709FFEC129B6FE
# depotFile //UE5/Release-5.0/Engine/Binaries/DotNET/CsvTools/CsvConvert.exe      headAction branch       fileSize 8192   digest 786BC09D6DD05C566AF40967DEAD3AC7

target = {}
source = {}

class DepotFile:
    def __init__(self, depotFile, action, fileSize, digest):
        self.depotFile = depotFile
        self.action = action
        self.fileSize = fileSize
        self.digest = digest
    
    def __repr__(self):
        return ("%s|%s|%s|%s" % (self.depotFile, self.action, self.fileSize, self.digest))

    def __eq__(self, other):
        return self.depotFile == other.depotFile and \
               self.fileSize == other.fileSize and \
               self.digest == other.digest
        
def readFile(fname):
    files = {}
    with open(fname, "r") as f:
        for line in f:
            line = line.rstrip()
            parts = line.split("\t")
            assert(len(parts) == 4)
            depotFile = parts[0].replace("depotFile ", "")
            action = parts[1].replace("headAction ", "")
            fileSize = parts[1].replace("fileSize ", "")
            digest = parts[1].replace("digest ", "")
            files[depotFile] = DepotFile(depotFile, action, fileSize, digest)
    return files

target = readFile("trel50.txt")
source = readFile("srel50.txt")

differences = {}
targetNotInSource = {}
sourceNotInTarget = {}

for k, v in target.items():
    if not k in source:
        targetNotInSource[k] = v
    elif v != source[k]:
        differences[k] = (v, source[k])

for k, v in source.items():
    if not k in source:
        targetNotInSource[k] = v
    elif v != source[k]:
        assert(k in differences)

print("Target not in Source:\n", "\n".join([str(targetNotInSource[x]) for x in targetNotInSource]))
print("")
print("Source not in Target:\n", "\n".join([str(sourceNotInTarget[x]) for x in sourceNotInTarget]))
print("")
print("Differences:")
for k, v in differences.items():
    print("Targ: %s\nSrc : %s\n" % (str(v[0]), str(v[1])))
