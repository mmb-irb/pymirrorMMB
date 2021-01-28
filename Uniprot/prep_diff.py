import sys
import gzip

file0 = sys.argv[1]
file1 = sys.argv[2]

if file0.find('.gz') != -1:
    fh_in = gzip.open(file0 ,'r')
else:
    fh_in = open(file0 ,'r')
    
list0 = set()

header_lines = True
for line in fh_in:
    if not isinstance(line, str):
       line = line.decode('ascii')
    header_lines = header_lines and (line.find('_____') == -1)
    if header_lines:
        continue
    if line.find('_____') != -1:
        continue
    if line=='':
        break
    line = line.rstrip()
    list0.add(line)
    
fh_in.close()
print(len(list0))
if file1.find('.gz') != -1:
    fh_in = gzip.open(file1 ,'r')
else:
    fh_in = open(file1 ,'r')


list = []
header_lines = True
for line in fh_in:
    if not isinstance(line, str):
       line = line.decode('ascii')
    header_lines = header_lines and (line.find('_____') == -1)
    if header_lines:
        continue
    if line.find('_____') != -1:
        continue
    line = line.rstrip()
    if line not in list0:
        list.append(line)
    if line=='':
        break

for line in list:
    print(line)
    
