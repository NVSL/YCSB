#!/usr/bin/python3
import subprocess, shlex
import pandas as pd

# execute a shell command, blocking, printing output
def shellcmd(cmdstr, stdout=subprocess.PIPE):
	cmds = shlex.split(cmdstr)
	result = subprocess.run(cmds, stdout=stdout)
	# print(result.stdout.decode("utf-8"))

def load(workload):
    ofname=workload+"_load.csv"
    cmd = "./bin/ycsb load kyotocabinet -s -P {wkld}".format(wkld=workload)
    print(cmd)
    with open(ofname, 'wb') as f:
        shellcmd(cmd, stdout=f)

def run(workload):
    ofname=workload+"_run.csv"
    cmd = "./bin/ycsb run kyotocabinet -s -P {wkld}".format(wkld=workload)
    print(cmd)
    with open(ofname, 'wb') as f:
        shellcmd(cmd, stdout=f)

def verify(workload):
    df = pd.read_csv(workload)
    l = ["Return" in f for f in df.iloc[:,1]]
    for ret in df.iloc[:,1][l]:
        if(ret.strip() != "Return=OK"):
            print(ret)
            return False
    return True

if __name__ == "__main__":
    workloads = range(ord('a'), ord('g'))
    for wkld in workloads:
        if(wkld == ord('e')):
            print("Passing workload e (scan)")
            continue
        print("Starting workload %c" % chr(wkld))
        print(''.join(['-'] * 100))
        workload = "./workloads/workload" + chr(wkld)
        load(workload)
        assert(verify(workload + "_load.csv"))

        run(workload)
        assert(verify(workload + "_run.csv"))
    print("No assertion error found. Done.")
