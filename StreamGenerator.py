#!/usr/bin/python
import sys, time

sourceData = sys.argv[1]
dataSubjectArea = sys.argv[3]
placeholder = "{}_LastLine.txt".format(dataSubjectArea)

def GetLineCount():
    with open(sourceData) as f:
        for i, l in enumerate(f):
            pass
    return i

def MakeLog(startLine, numLines):
    destData = time.strftime("/tmp/aths-kinesis-stream/{}_%Y%m%d-%H%M%S.log".format(dataSubjectArea))
    with open(sourceData, 'r') as jsonfile:
        with open(destData, 'w+') as dstfile:

            inputRow = 0
            linesWritten = 0
            for line in jsonfile:
                inputRow += 1
                if (inputRow > startLine):
                    dstfile.write(line)
                    linesWritten += 1
                    if (linesWritten >= numLines):
                        break
            return linesWritten
        
    
numLines = 100
startLine = 0            
if (len(sys.argv) > 1):
    numLines = int(sys.argv[2])
    
try:
    with open(placeholder, 'r') as f:
        for line in f:
             startLine = int(line)
except IOError:
    startLine = 0

print("Writing " + str(numLines) + " lines starting at line " + str(startLine) + "\n")

totalLinesWritten = 0
linesInFile = GetLineCount()

while (totalLinesWritten < numLines):
    linesWritten = MakeLog(startLine, numLines - totalLinesWritten)
    totalLinesWritten += linesWritten
    startLine += linesWritten
    if (startLine >= linesInFile):
        startLine = 0
        
print("Wrote " + str(totalLinesWritten) + " lines.\n")
    
with open(placeholder, 'w') as f:
    f.write(str(startLine))