from site_parser import GatherByTag
from subprocess import run
from os import listdir

class DFSConfig():
    def __init__(self) -> None:
        self.inputFolder = "bot-in"
        self.outputFolder = "bot-out"
        self.resultsFolder = "bot-results"
        self.defaultSettings = {
            "query": "any",
            "depth": 1000,
            "trunc": 10
        }


def main():

    cfg = DFSConfig()

    # setup folders
    run(f"mkdir -p {cfg.inputFolder} {cfg.outputFolder} {cfg.resultsFolder}", shell=True)
    run(f"hdfs dfs -mkdir -p {cfg.inputFolder} {cfg.outputFolder}", shell=True)

    exit_ = False
    while not exit_:

        # await task
        command = input("[input@bot]~$ ")

        if command == "exit":
            exit_ = True
            continue
    
        if command == "help":
            print("stop execution : exit")
            print("exec wordcount : wordcount [search-depth] [question-tag] OR wc [search-depth] [question-tag]")
        
        if command[:9] == "wordcount" or command[:2] == "wc":
            args = command.split()
            query, depth = cfg.defaultSettings["query"], cfg.defaultSettings["depth"]
            try:
                depth = int(args[1])
                query = args[2]
            except Exception:
                print("wrong argument values, expected int, string")
            
            possibleName = f"{query}-{depth}.result"
            if possibleName in listdir("bot-results"):
                print(f"some results are already stored in {possibleName}")
                inp = ""
                allowed = ["y", "n", "yes", "no"]
                while inp.lower() not in allowed:
                    inp = input("would you like to update results now (y/n): ")
                if inp in ["n", "no"]:
                    continue
                print("updating now")

            texts = GatherByTag(query, depth)
            if len(texts) <= 5:
                print(f"too few questions found ({len(texts)}) try increasing search depth")
                continue
            print(f"found ({len(texts)}) questions, creating mapred task")

            # reset local workspace
            run(f"rm -rf {cfg.inputFolder} {cfg.outputFolder}", shell=True)
            run(f"mkdir {cfg.inputFolder}", shell=True)

            # create a file with current task
            with open(f"{cfg.inputFolder}/{query}.task", "w") as writer:
                for text in texts:
                    writer.write(text)
            
            # reset worspace
            run(f"hdfs dfs -rm -r -f {cfg.inputFolder} {cfg.outputFolder}", shell=True)
            run(f"hdfs dfs -mkdir -p {cfg.inputFolder}", shell=True)

            # upload task to hdfs
            run(f"hdfs dfs -put {cfg.inputFolder}/{query}.task {cfg.inputFolder}", shell=True)

            # run mapred task
            run(f'mapred streaming -input {cfg.inputFolder}/{query}.task -output {cfg.outputFolder} -mapper "python3 mapper.py" -reducer "python3 reducer.py" -file mapper.py -file reducer.py', shell=True)

            # download result
            run(f'hdfs dfs -get {cfg.outputFolder} {cfg.outputFolder}', shell=True)

            # assemble response
            lines = []
            for filename in listdir("bot-out"):
                if filename[:5] == "part-":
                    with open(f"bot-out/{filename}", "r") as reader:
                        lines.extend(reader.readlines())
            
            with open(f"{cfg.resultsFolder}/{query}-{depth}.result", "w") as writer:
                for line in lines:
                    writer.write(line)

            print(f"task completed, look for results locally in {cfg.resultsFolder}/{query}.result")
            print(f"head of {query}.result reads:")
            for i in range(cfg.defaultSettings["trunc"]):
                print(lines[i].strip())
            
if __name__ == "__main__":
    main()



