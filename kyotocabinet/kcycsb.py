#!/usr/bin/python3
import os, shutil, argparse
import subprocess, shlex, datetime
import pandas as pd
from argparse import RawTextHelpFormatter

def parse_arguments():
	argparser = argparse.ArgumentParser(formatter_class=RawTextHelpFormatter)
	argparser.add_argument("--workload", default=range(ord('a'), ord('g')), dest="workloads",
						required=False, help="kyotocabinet operation count")
	argparser.add_argument("--op_cnt", default=1000, dest="op_cnt",
						required=False, help="kyotocabinet operation count")
	argparser.add_argument("--rec_cnt", default=1000, dest="rec_cnt",
						required=False, help="kyotocabinet record count")
	argparser.add_argument("--kcdir", default="/mnt/ramdisk/tmp.kch", dest="kcdir",
						required=False, help="kyotocabinet db file")
	argparser.add_argument("--fs", default=os.environ["SUBZERO_TARGET_FS"], dest="fs",
						required=False, help="filesystem to use")
	argparser.add_argument("--dev", default="/dev/pmem2", dest="dev",
						required=False, help="device name")
	argparser.add_argument("--mnt", default="/mnt/ramdisk", dest="mnt",
						required=False, help="mount location")
	argparser.add_argument("--logdir", default=timestamp(), dest="logdir",
						required=False, help="log file name")
	return argparser.parse_args()

# execute a shell command, blocking, printing output
def shellcmd(cmdstr, stdout=subprocess.PIPE):
	cmds = shlex.split(cmdstr)
	result = subprocess.run(cmds, stdout=stdout)
	# print(result.stdout.decode("utf-8"))

def remount(args):
	cmd="mount_fs.sh {fs} {dev} {mnt}".format(fs=args.fs, dev=args.dev, mnt=args.mnt)
	print("Remounting: %s" % (cmd))
	shellcmd(cmd)

def save_ts(args, tsname):
	ts_file="/proc/fs/NOVA/{dev}/timing_stats".format(dev=os.path.basename(args.dev))
	shutil.copyfile(ts_file, tsname)

def timestamp():
	return datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

def create_logdir(args):
	os.mkdir(args.logdir)

def load(args, workload):
	ofname=workload+"_load.csv"
	tsname = "{}/{}.ts".format(args.logdir, os.path.basename(workload + "_load"))
	cmd = "./bin/ycsb load kyotocabinet -s -P {wkld} -p recordcount={rec} -p operationcount={op} -p kc.dir={kcdir}".format(
		wkld=workload, rec=args.rec_cnt, op=args.op_cnt, kcdir=args.kcdir)
	print(cmd)
	with open(ofname, 'wb') as f:
		shellcmd(cmd, stdout=f)
	save_ts(args, tsname)

def run(args, workload):
	ofname=workload+"_run.csv"
	tsname = "{}/{}.ts".format(args.logdir, os.path.basename(workload + "_run"))
	cmd = "./bin/ycsb run kyotocabinet -s -P {wkld} -p recordcount={rec} -p operationcount={op} -p kc.dir={kcdir}".format(
		wkld=workload, rec=args.rec_cnt, op=args.op_cnt, kcdir=args.kcdir)
	print(cmd)
	with open(ofname, 'wb') as f:
		shellcmd(cmd, stdout=f)

def verify(workload):
	df = pd.read_csv(workload, header=None)
	l = ["Return" in f for f in df.iloc[:,1]]
	for ret in df.iloc[:,1][l]:
		if(ret.strip() != "Return=OK"):
			print(ret)
			return False
	return True

def stats(workload):
	df = pd.read_csv(workload, header=None)
	df[0] = df[0].str.strip()
	df[1] = df[1].str.strip()
	return df

if __name__ == "__main__":
	args = parse_arguments()
	create_logdir(args)
	fail = []
	okay = []
	workloads = range(ord('a'), ord('g'))
	df = []

	result = {}
	for wkld in workloads:
		if(wkld == ord('e')):
			print("Passing workload e (scan)")
			continue
		print("Starting workload %c" % chr(wkld))
		remount(args)
		print(''.join(['-'] * 100))
		workload = "./workloads/workload" + chr(wkld)
		load(args, workload)
		if(not verify(workload + "_load.csv")):
			fail.append(workload + "_load.csv")
			continue

		run(args, workload)
		if(not verify(workload + "_run.csv")):
			fail.append(workload + "_run.csv")
			continue

		okay.append(workload + "_load.csv")
		okay.append(workload + "_run.csv")

		df = stats(workload + "_run.csv")

		operations = ['[READ]', '[INSERT]', '[CLEANUP]', '[UPDATE]', '[READ-MODIFY-WRITE]']
		found_ops = pd.unique(df[0])
		headers = ['Operations', 'MinLatency(us)', 'AverageLatency(us)', '95thPercentileLatency(us)', '99thPercentileLatency(us)', 'MaxLatency(us)']

		print(','.join(headers))
		wkld_vals = {}
		for op in operations:
			if (op not in found_ops):
				continue
			op_vals = []
			for head in headers:
				op_vals.append(float(df[(df[0] == op) & (df[1] == head)][2]))
			wkld_vals[op] = op_vals
		result[wkld] = wkld_vals

	print(''.join(['='] * 100))
	print(args)
	print("Fail: " + ','.join(fail))
	print("Okay: " + ','.join(okay))
	print(result)


