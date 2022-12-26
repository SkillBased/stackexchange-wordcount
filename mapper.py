from sys import stdin

REQUIREDCHARACTERS = "abcdefghijklmnopqrstuvwxyz"
ALLOWEDCHARACTERS = "1234567890-"
DISMISSEDCHARACTERS = '!?,.:;"'

def ProcessWord(word : str) -> str:
    '''
    returns a word without syntax characters if word is indeed a word
    retruns empty string if word is in fact a formula or a number
    '''
    processedWord = ""
    wordOK = False
    for character in word:
        if character in REQUIREDCHARACTERS:
            wordOK = True
            processedWord += character
        elif character in ALLOWEDCHARACTERS:
            processedWord += character
        elif character in DISMISSEDCHARACTERS:
            continue
        else:
            return ""
    if wordOK:
        # corner case for -...
        if processedWord[0] == "-":
            return processedWord[1:]
        return processedWord
    return ""

for text in stdin:
    for word in text.split(" "):
        processed = ProcessWord(word.lower())
        if processed != "":
            print(f"{processed}\t1")