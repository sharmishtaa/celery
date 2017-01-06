from celery import Celery
import subprocess
import os
import time
from contextlib import contextmanager
from celery.utils.log import get_task_logger


app = Celery('tasks', backend='amqp', broker='amqp://sharmi:sharmi@10.128.24.20:5672//')
#app = Celery('tasks', backend='amqp', broker='amqp://guest:guest@localhost:5672//')
CELERY_REDIRECT_STDOUTS = True

@contextmanager
def cd(newdir):
    prevdir = os.getcwd()
    os.chdir(os.path.expanduser(newdir))
    try:
        yield
    finally:
        os.chdir(prevdir)

@app.task(ignore_result=True)
def run_alignment(fname, mydir):
    logger = get_task_logger(__name__)
    print os.getcwd()
    with cd(mydir):
        print mydir
	(prefix,dot,ext)=fname.partition('.')
	logfile = fname+".log"
	strcmd = "sh "+fname+ " > " + logfile	
	print strcmd
	os.system(strcmd)   

@app.task(ignore_result=True)
def run_makecmd(cmd, mydir):
    with cd(mydir):
	strcmd = "make "+cmd 
	print mydir
	print strcmd
	os.system(strcmd)   

@app.task(ignore_result=True)
def run_celerycommand(cmd, mydir):
    with cd(mydir):
	print mydir
	print cmd
	os.system(cmd) 	


@app.task(ignore_result=True)
def testfunction(fname):
    logger = get_task_logger(__name__)
    with cd("../processed/jobs/"):
	(prefix,dot,ext)=fname.partition('.')
	logfile = prefix+".log"
	strcmd = "sh "+fname+ " > " + logfile	
	print strcmd
	time.sleep(20)
	#os.system(strcmd)  
    
