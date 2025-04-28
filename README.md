# ai-python-project-4--hdfs-replication-solved
**TO GET THIS SOLUTION VISIT:** [AI-python Project 4- HDFS Replication Solved](https://www.ankitcodinghub.com/product/python-p4-6-of-grade-hdfs-replication-solved/)


---

📩 **If you need this solution or have special requests:** **Email:** ankitcoding@gmail.com  
📱 **WhatsApp:** +1 419 877 7882  
📄 **Get a quote instantly using this form:** [Ask Homework Questions](https://www.ankitcodinghub.com/services/ask-homework-questions/)

*We deliver fast, professional, and affordable academic help.*

---

<h2>Description</h2>



<div class="kk-star-ratings kksr-auto kksr-align-center kksr-valign-top" data-payload="{&quot;align&quot;:&quot;center&quot;,&quot;id&quot;:&quot;119424&quot;,&quot;slug&quot;:&quot;default&quot;,&quot;valign&quot;:&quot;top&quot;,&quot;ignore&quot;:&quot;&quot;,&quot;reference&quot;:&quot;auto&quot;,&quot;class&quot;:&quot;&quot;,&quot;count&quot;:&quot;1&quot;,&quot;legendonly&quot;:&quot;&quot;,&quot;readonly&quot;:&quot;&quot;,&quot;score&quot;:&quot;5&quot;,&quot;starsonly&quot;:&quot;&quot;,&quot;best&quot;:&quot;5&quot;,&quot;gap&quot;:&quot;4&quot;,&quot;greet&quot;:&quot;Rate this product&quot;,&quot;legend&quot;:&quot;5\/5 - (1 vote)&quot;,&quot;size&quot;:&quot;24&quot;,&quot;title&quot;:&quot;AI-python  Project 4- HDFS Replication Solved&quot;,&quot;width&quot;:&quot;138&quot;,&quot;_legend&quot;:&quot;{score}\/{best} - ({count} {votes})&quot;,&quot;font_factor&quot;:&quot;1.25&quot;}">

<div class="kksr-stars">

<div class="kksr-stars-inactive">
            <div class="kksr-star" data-star="1" style="padding-right: 4px">


<div class="kksr-icon" style="width: 24px; height: 24px;"></div>
        </div>
            <div class="kksr-star" data-star="2" style="padding-right: 4px">


<div class="kksr-icon" style="width: 24px; height: 24px;"></div>
        </div>
            <div class="kksr-star" data-star="3" style="padding-right: 4px">


<div class="kksr-icon" style="width: 24px; height: 24px;"></div>
        </div>
            <div class="kksr-star" data-star="4" style="padding-right: 4px">


<div class="kksr-icon" style="width: 24px; height: 24px;"></div>
        </div>
            <div class="kksr-star" data-star="5" style="padding-right: 4px">


<div class="kksr-icon" style="width: 24px; height: 24px;"></div>
        </div>
    </div>

<div class="kksr-stars-active" style="width: 138px;">
            <div class="kksr-star" style="padding-right: 4px">


<div class="kksr-icon" style="width: 24px; height: 24px;"></div>
        </div>
            <div class="kksr-star" style="padding-right: 4px">


<div class="kksr-icon" style="width: 24px; height: 24px;"></div>
        </div>
            <div class="kksr-star" style="padding-right: 4px">


<div class="kksr-icon" style="width: 24px; height: 24px;"></div>
        </div>
            <div class="kksr-star" style="padding-right: 4px">


<div class="kksr-icon" style="width: 24px; height: 24px;"></div>
        </div>
            <div class="kksr-star" style="padding-right: 4px">


<div class="kksr-icon" style="width: 24px; height: 24px;"></div>
        </div>
    </div>
</div>


<div class="kksr-legend" style="font-size: 19.2px;">
            5/5 - (1 vote)    </div>
    </div>
Overview

HDFS can partition large files into blocks to share the storage across many workers, and it can replicate those blocks so that data is not lost even if some workers die.

If you switched machines, remember to reinstall Docker and also to enable Docker to be run without sudo. Refer to P1 for instructions.

Learning objectives: * use the HDFS command line client to upload files * use the webhdfs API (https://hadoop.apache.org/docs/r3.3.6/hadoopproject-dist/hadoop-hdfs/WebHDFS.html) to read files * use PyArrow to read HDFS files * relate replication count to space efficiency and fault tolerance

Before starting, please review the general project directions.

Corrections/Clarifications

Oct. 18: Added link to tutorial to change instance type

Oct. 18: added step to create .gitignore file to save students time

Oct. 25: Updated notebook.Dockerfile to use older version of jupyter client

Part 1: Deployment and Data Upload

Before you begin, please run the below command in your p4 directory. This will stop git from trying to track csv files which will save you a lot of headaches (This step will not be graded – it is just to help you).

echo “*.csv” &gt;&gt; .gitignore Cluster

For this project, you’ll deploy a small cluster of containers, one with Jupyter, one with an HDFS NameNode, and two with HDFS DataNodes.

We have given you docker-compose.yml for starting the cluster, but you need to build some images first. Start with the following:

docker build . -f hdfs.Dockerfile -t p4-hdfs docker build . -f notebook.Dockerfile -t p4-nb

The second image depends on the first one (p4-hdfs) allowing us to avoid repeating imports –you can see this by checking the FROM line in “notebook.Dockerfile”.

The compose file also needs p4-nn (NameNode) and p4-dn (DataNode) images. Create Dockerfiles for these that can be built like this:

docker build . -f namenode.Dockerfile -t p4-nn docker build . -f datanode.Dockerfile -t p4-dn

Requirements: * like p4-nb, both these should use p4-hdfs as a base * namenode.Dockerfile should run two commands, hdfs namenode

-format and hdfs namenode -D dfs.namenode.stale.datanode.interval=10000 -D dfs.namenode.heartbeat.recheckinterval=30000 -fs hdfs://boss:9000 * datanode.Dockerfile should just run hdfs datanode -D dfs.datanode.data.dir=/var/datanode -fs hdfs://boss:9000

You can use docker compose up -d to start your mini cluster. You can run docker compose kill; docker compose rm -f to stop and delete all the containers in your cluster as needed. For simplicity, we recommend this rather than restarting a single container when you need to change something as it avoids some tricky issues with HDFS. For example, if you just restart+reformat the container with the NameNode, the old DataNodes will not work with the new NameNode without a more complicated process/config.

Data Upload

Connect to JupyterLab running in the p4-nb container, and create a notebook called p4a.ipynb in the “/nb” directory (we’ll do some later work in another notebook, p4b.ipynb). Note that juypterlab is being hosted on port 5000 instead of 5440

Q1: how many live DataNodes are in the cluster?

Write a cell like this (and use a similar format for other questions):

“`

q1

! SHELL COMMAND TO CHECK “`

The shell command should generate a report by passing some arguments to hdfs dfsadmin. The output should contain a line like this (you might need to wait and re-run this command a bit to give the DataNodes time to show up):

… Live datanodes (2): …

Next, use two hdfs dfs -cp commands to upload this same file to HDFS twice, to the following locations:

hdfs://boss:9000/single.csv hdfs://boss:9000/double.csv

In both cases, use a 1MB block size (dfs.block.size), and replication (dfs.replication) of 1 and 2 for single.csv and double.csv, respectively.

If you want to re-run your notebook from the top and have the files re-created, consider having a cell with the following, prior to the cp commands.

!hdfs dfs -rm -f hdfs://boss:9000/single.csv !hdfs dfs -rm -f hdfs://boss:9000/double.csv

Q2: what are the logical and physical sizes of the CSV files?

Run a du command with hdfs dfs to see.

You should see something like this:

166.8 M 333.7 M hdfs://main:9000/double.csv 166.8 M 166.8 M hdfs://main:9000/single.csv

Part 2: WebHDFS

The documents here describe how we can interact with HDFS via web requests: https://hadoop.apache.org/docs/r3.3.6/hadoop-projectdist/hadoop-hdfs/WebHDFS.html.

Many examples show these web requests being made with the curl command, but you’ll adapt those examples to use requests.get (https://requests.readthedocs.io/en/latest/user/quickstart/).

By default, WebHDFS runs on port 9870. So use port 9870 instead of 9000 for this part.

Q3: what is the file status for single.csv?

Use the GETFILESTATUS operation to find out, and answer with a dictionary (the request returns JSON).

Note that if r is a response object, then r.content will contain some bytes, which you could convert to a dictionary; alternatively, r.json() does this for you.

The result should look something like this:

{‘FileStatus’: {… ‘blockSize’: 1048576, … ‘length’: 174944099, … ‘replication’: 1, ‘storagePolicy’: 0, ‘type’: ‘FILE’}}

The blockSize and length fields might be helpful for future questions.

Q4: what is the location for the first block of single.csv?

Use the OPEN operation with offset 0 and noredirect=true – (length and buffersize are optional). You answer should a string, similar to this:

python ‘http://6a2464e4ba5c:9864/webhdfs/v1/single.csv?op=OPEN&amp;namenoderpcaddress=boss:9000&amp;offset=0’

Note that 6a2464e4ba5c was the randomly generated container ID for the container running the DataNode, so yours will be different.

Q5: how are the blocks of single.csv distributed across the two DataNode containers?

This is similar to above, except you should check every block andextract the container ID from the URL.

You should produce a Python dictionary similar to below (your IDs and counts will be different, of course).

python {‘6a2464e4ba5c’: 88, ‘805fe2ba2d15’: 79}

If all the blocks are on the same DataNode, it is likely you uploaded the CSV before both DataNodes got a chance to connect with the NameNode. Re-run, this time giving the cluster more time to come up.

Part 3: PyArrow

Q6: what are the first 10 bytes of single.csv?

Use PyArrow to read the HFDS file. You can connect to HDFS like this (the missing values are host and port, respectively):

Hint: Think about which port we should connect on. python import pyarrow as pa import pyarrow.fs hdfs = pa.fs.HadoopFileSystem(????, ????)

You can then use hdfs.open_input_file(????) with a path to open the file and return a pyarrow.lib.NativeFile object.

You can use the read_at call on a NativeFile to get a specified number of bytes at a specified offset.

Output the first 10 bytes of single.csv.

Q7: how many lines of single.csv contain the string “Single Family”?

PyArrow’s NativeFile implements the RawIOBase interface (even though it is not a subclass): https://docs.python.org/3/library/io.html#io.RawIOBase.

This means NativeFile can read bytes at some location into a buffer, but it doesn’t do other nice things for you like letting you loop over lines or converting bytes to text. The io module has some wrappers you could optionally use to get this functionality, such as:

https://docs.python.org/3/library/io.html#io.BufferedReader https://docs.python.org/3/library/io.html#io.TextIOWrapper

Part 4: Disaster Strikes

Do the following: * manually kill one of the DataNode containers with a docker kill command * start a new notebook in Jupyter called p4b.ipynb — use it for the remainder of your work

Q8: how many live DataNodes are in the cluster?

This is the same question as Q1, but now there should only be one:

… Live datanodes (1) …

You might need to wait a couple minutes and re-run this until the NameNode recognizes that the DataNode has died.

Important – Add the below line to a cell below q8 but before q9. This is for the autograder. You do not need to run it. import time

time.sleep(30)

Q9: how are the blocks of single.csv distributed across the DataNode containers?

This is the same as Q5, but you’ll need to do a little extra work. When you make a request to the NameNode, check the status code (r.status_code). If it is 403, use “lost” as the key for your count; otherwise, count as normal.

Your output should look something like the following (again, your container ID and the exact counts will vary):

python {‘c9e70330e663’: 86, ‘lost’: 81}

Q10: how many times does the text “Single Family” appear in the remaining blocks of single.csv?

There are different ways of extracting the data that is still intact — one approach would be to (a) identify good blocks using code from Q9 and (b) read the bytes for those healthy blocks using a read call similar to the one in Q6.

Once you decide on a solution, loop through single.csv, count the number of times “Single Family” appears in the remaining blocks of single.csv, and then output that count.

Submission

We should be able to run the following on your submission to create the mini cluster:

docker build . -f hdfs.Dockerfile -t p4-hdfs docker build . -f namenode.Dockerfile -t p4-nn docker build . -f datanode.Dockerfile -t p4-dn docker build . -f notebook.Dockerfile -t p4-nb docker compose up -d

We should then be able to open http://localhost:5000/lab and find your p4a.ipynb and p4b.ipynb notebooks and run them.

To make sure you didn’t forget to push anything, we recommend doing a git clone of your repo to a new location and going through these steps as a last check on your submission.

You’re free to create and include other code files if you like (for example, you could write a .py module used by both notebooks).

Tester:

Expected that you use Python 3.10.12

After you push your final submission, try cloning your repo into a new temp folder and run the test there; this will simulate how we run the tests during grading.

Copy in tester.py from the main github directory into your p4 folder

You can run python3 autograde.py -g to create a debug directory which will contain the notebooks that were used for testing. This will let you examine the state of the notebooks and catch errors

Make sure your answers are in cell output – not print statements (see the example below) my_answer = [] for i in range(5):

my_answer.append(5) my_answer
