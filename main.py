from site_parser import GatherByTag, ProcessPage
from subprocess import run
from os import listdir
from time import localtime, asctime

class DFSConfig():
    def __init__(self) -> None:
        self.inputFolder = "bot-in"
        self.outputFolder = "bot-out"
        self.resultsFolder = "bot-results"

        self.defaultSettings = {
            "query":        "any",
            "depth":        200,    # pages
            "trunc":        10,
            "updatetimer":  3600    # 1 hour
        }

        self.sourceURL = "https://math.stackexchange.com/questions?tab=newest&pagesize=50&page="
        self.lastQuestionId = ""
        self.timeFormat = "%Y-%m-%dT%H:%M:%SZ"
        self.lastUpdated = localtime()


cfg = DFSConfig()

def UpdateData(drop : bool = False) -> None:
    global cfg
    print(f"running database update, previous update time: {asctime(cfg.lastUpdated)}")
    # reset local workspace
    run(f"rm -rf {cfg.inputFolder} {cfg.outputFolder}", shell=True)
    run(f"mkdir {cfg.inputFolder}", shell=True)

    print("querying source for posts")

    batchFirst = None
    stacks = {}
    page = 1
    getNext = True
    while getNext and page <= cfg.defaultSettings["depth"]:
        items = ProcessPage(cfg.sourceURL + str(page))
        for post in items:
            if batchFirst is None:
                batchFirst = post.id
            if post.id == cfg.lastQuestionId:
                getNext = False
                break
            for tag in post.tags:
                if stacks.get(tag) is None:
                    stacks[tag] = []
                stacks[tag].append(post.text)
        page += 1

    print(f"pulled {page-1} pages, reached last record")

    if drop:
        run(f"hdfs dfs -rm -r -f {cfg.inputFolder}", shell=True)

    for tag in sorted(stacks.keys()):
        print(f"updating {cfg.inputFolder}/{tag}.data")
        # create an update for dfs file
        with open(f"{cfg.inputFolder}/{tag}.part", "w") as writer:
                for text in stacks[tag]:
                    writer.write(text + "\n")
        # upload update; appendToFile creates file if it didn't exist
        run(f"hdfs dfs -appendToFile {cfg.inputFolder}/{tag}.part {cfg.inputFolder}/{tag}.data", shell=True)
        run(f"hdfs dfs -appendToFile {cfg.inputFolder}/{tag}.part {cfg.inputFolder}/any.data", shell=True)

    cfg.lastQuestionId = batchFirst
    cfg.lastUpdated = localtime()
    print("update complete")

def main():
    global cfg

    # setup folders
    run(f"mkdir -p {cfg.inputFolder} {cfg.outputFolder} {cfg.resultsFolder}", shell=True)
    run(f"hdfs dfs -mkdir -p {cfg.inputFolder} {cfg.outputFolder}", shell=True)

    # run full reset update on entry
    UpdateData(drop=True)

    exit_ = False
    while not exit_:

        # passively update every so often

        # await task
        command = input("[input@bot]~$ ")

        if command == "exit":
            exit_ = True
            continue
    
        if command == "help":
            print("stop execution  : exit")
            print("update database : update")
            print("exec wordcount  : wordcount [question-tag] OR wc [question-tag]")

        if command == "update":
            UpdateData()

        if command[:9] == "wordcount" or command[:2] == "wc":
            args = command.split()
            query = cfg.defaultSettings["query"]
            if len(args > 2):
                print("wrong command expected wordcount (query) or wc (query)")
                continue
            else:
                query = args[1]
            
            # reset local workspace
            run(f"rm -rf {cfg.outputFolder}", shell=True)
            
            # reset worspace
            run(f"hdfs dfs -rm -r -f {cfg.outputFolder}", shell=True)

            # run mapred task
            run(f'mapred streaming -input {cfg.inputFolder}/{query}.data -output {cfg.outputFolder} -mapper "python3 mapper.py" -reducer "python3 reducer.py" -file mapper.py -file reducer.py', shell=True)

            # download result
            run(f'hdfs dfs -get {cfg.outputFolder} {cfg.outputFolder}', shell=True)

            # assemble response
            lines = []
            for filename in listdir("bot-out"):
                if filename[:5] == "part-":
                    with open(f"bot-out/{filename}", "r") as reader:
                        lines.extend(reader.readlines())
            
            with open(f"{cfg.resultsFolder}/{query}.result", "w") as writer:
                for line in lines:
                    writer.write(line)

            print(f"task completed, look for results locally in {cfg.resultsFolder}/{query}.result")
            print(f"head of {query}.result reads:")
            for i in range(cfg.defaultSettings["trunc"]):
                print(lines[i].strip())
            
if __name__ == "__main__":
    main()



