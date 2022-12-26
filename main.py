from site_parser import GatherByTag
from subprocess import run

TMPDIR = "bot-temporary"
RESDIR = "bot-results"

DEFAULTQUERY = ""
DEFAULTDEPTH = 5000

def main():

    # reset results on startup
    run(f"rm -rf ./{RESDIR}")
    run(f"mkdir ./{RESDIR}")

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
            query, depth = DEFAULTQUERY, DEFAULTDEPTH
            try:
                depth = int(args[1])
                query = args[2]
            except Exception:
                print("wrong argument values, expected int, string")
            
            texts = GatherByTag(query, depth)
            if len(texts) <= 5:
                print(f"too few questions found ({len(texts)}) try increasing search depth")
                continue
            print(f"found ({len(texts)}) questions, creating mapred task")

            # create a file with current task
            with open(f"{TMPDIR}/{query}.task", "w") as writer:
                for text in texts:
                    writer.write(text)
            
            # reset hdfs workspace
            run(f'hdfs dfs -rm -r {TMPDIR} {RESDIR}')
            # upload task to hdfs
            run(f'hdfs dfs -copyFromLocal {TMPDIR}/{query}.task {TMPDIR}')
            # run mapred task
            mapredTask = f"""
            yarn jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar\ 
            -input {TMPDIR}\ 
            -output {RESDIR}\ 
            -file mapper.py\ 
            -file reducer.py\ 
            -mapper "python mapper.py"\ 
            -reducer "python reducer.py"
            """
            run(mapredTask)
            # download result
            run(f'hdfs dfs -copyToLocal {RESDIR}')

            print("task completed, look for results locally")
            




