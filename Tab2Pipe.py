inputFile = open(r"K:\Software.lib\TEST\20201109\tabbed\FAX_20201016-20201031.txt", "r")
exportFile = open(r"K:\Software.lib\TEST\20201109\piped\FAX_20201016-20201031.csv", "w")
for line in inputFile:
    new_line = line.replace('\t', '|')
    exportFile.write(new_line)

inputFile.close()
exportFile.close()