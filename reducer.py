from sys import stdin

currentWord = None
count = 0

for line in stdin:
    word, value = line.strip().split("\t") 
    if word != currentWord and currentWord is not None: 
        print(f"{currentWord}\t{count}") 
        count = 0
    currentWord = word 
    count += int(value)
 
if currentWord is not None: 
        print(f"{currentWord}\t{count}") 
