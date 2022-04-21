Hadoop mrJob sentiment Analysis
cluster:
    1 namenode + 3 workers
    node specs:
        RAM: 4GB
        VCPUs: 2 VCPU
        Disk: 40GB
User time (seconds): 35.21
System time (seconds): 5.28
Percent of CPU this job got: 1%
Elapsed (wall clock) time (h:mm:ss or m:ss): 42:33.72
Average shared text size (kbytes): 0
Average unshared data size (kbytes): 0
Average stack size (kbytes): 0
Average total size (kbytes): 0
Maximum resident set size (kbytes): 273448
Average resident set size (kbytes): 0
Major (requiring I/O) page faults: 0
Minor (reclaiming a frame) page faults: 412469
Voluntary context switches: 110791
Involuntary context switches: 17563
Swaps: 0
File system inputs: 0
File system outputs: 6424
Socket messages sent: 0
Socket messages received: 0
Signals delivered: 0
Page size (bytes): 4096
Exit status: 0

notes:
    cleaning technic re lib, regEx
    AFFINN word weight dictionary used in python dict
    tokenized comments are stored in python dictionary
    python statistics lib used for calcualting mean