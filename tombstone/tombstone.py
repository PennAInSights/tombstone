import os
import argparse
import datetime
import time
import logging
import json
import pika

class Tombstone:
    def __init__(self, directory=None, level=1, depth=0, threshold=None):

        # base directory to start monitoring
        self._directory = directory

        # Name of tombstone file
        self._filename = "download_complete.txt"

        # monitor subdirs this many levels below the base
        self._level = level

        # how many levels for subdirs to examine for a monitored directory
        self._depth = depth

        # most recent list of monitored directories and ages
        self._monitor=None

        # most recent list of static directories without a tombstone
        self._static=None

        # Age (in seconds) used to trigger the creation of a tombstone
        self._threshold=threshold

        # Check all files, not just directories
        self._files=False

        # Queue name for RabbitMQ messages
        self._queue = None

    @property
    def depth(self):
        return self._depth
    
    @depth.setter
    def depth(self, value: int):
        self._depth = value

    @property
    def directory(self):
        return self._directory
    
    @directory.setter
    def directory(self, value: str):
        if not os.path.isdir(value):
            raise ValueError(f'Not a directory: {value}')
        self._directory = value

    @property
    def filename(self):
        return self._filename
    
    @filename.setter
    def filename(self, value: str):
        self._filename = value

    @property
    def files(self):
        return self._files

    @files.setter
    def files(self, value: bool):
        self._files = value

    @property
    def level(self):
        return self._level
    
    @level.setter
    def level(self, value: int):
        self._level = value

    @property
    def monitor(self):
        return self._monitor

    @monitor.setter
    def monitor(self, value: list | None):
        self._monitor = value

    @property
    def static(self):
        return self._static
    
    @static.setter
    def static(self, value: list | None):
        self._static = value

    @property
    def threshold(self):
        return self._threshold
    
    @threshold.setter
    def threshold(self, value: int | None):
        if value < 0:
            raise ValueError(f'Invalid threhsold: {value}')
        self._threshold = value  

    @property
    def queue(self):
        return self._queue      

    @queue.setter
    def queue(self, value: list | None):
        self._queue=value

    # walk through a directory up to the specified depth of levels
    def walk_to_depth(self, directory: str, depth: int):

        directory = os.path.normpath(directory)
        assert os.path.isdir(directory)

        base_level = directory.count(os.path.sep)
        for root, dirs, files in os.walk(directory):
            current_level = root.count(os.path.sep)
            yield root, dirs, files
            if (current_level - base_level) >= depth:
                del dirs[:]


    # Get the list of directories to monitor. These are subdirectories that
    #   are the specified number of levels below the base directory
    #   and do not currently contain a tombstone file
    def get_dirs_list(self, directories: list, levels: int, continuous: bool):
        dlevel = 0
        outlist = []
        finished = False
        dlist = directories

        while (dlevel < levels) and not finished:

            ilist = []
            for d in dlist:
                ilist = ilist + [os.path.join(d,x) for x in os.listdir(d) if os.path.isdir(os.path.join(d,x))]

            finished = len(ilist) == 0 

            dlevel = dlevel + 1
            if not finished:
                if continuous:
                    outlist = outlist + ilist
                else:
                    if dlevel == levels:
                        outlist = ilist
                
            dlist = ilist

        if self.filename is not None:   
            outlist = [x for x in outlist if not os.path.exists(os.path.join(x,self.filename))]
        outlist = [ {'name':x, 'age': os.path.getmtime(x)} for x in outlist ]

        return(outlist)

    def make_obituary(self, tombname):

        content = {
            "sender" : "tombstone",
            "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "filename": tombname
            }

        msg={"role": "user", "content": content}
        msg_str = json.dumps(msg)
        return(msg_str)

    def send_obituary(self, obituary):

        if self.queue is not None:


            connection = pika.BlockingConnection( 
                    pika.ConnectionParameters(
                        host=self.queue[0], 
                        port=self.queue[1]))

            channel = connection.channel()
            channel.queue_declare(queue=self.queue[2])
            channel.basic_publish(exchange='', routing_key=self.queue[2], body=obituary)
            connection.close()

    def update(self, make_tombstones=True):

        # current timestamp for reference
        timestamp = datetime.datetime.now().timestamp()

        # If only monitoring the base directory
        if self._level == 0:
             return([[self._directory, timestamp-os.path.getmtime(self._directory)]])
        
        # get last mod time for directories of interest
        self.monitor = self.get_dirs_list([self.directory], self.level, False)

        # Get age in seconds of each directory
        for d in range(len(self.monitor)):
            self.monitor[d]['age'] = timestamp - self.monitor[d]['age']

        # Examine subdirectories and files for more recent mtimes
        for d in range(len(self.monitor)):

            for root, dirs, files in self.walk_to_depth(self.monitor[d]['name'], self.depth):

                # get age for all subdirectories
                for subdir in dirs:
                    age = timestamp - os.path.getmtime(os.path.join(root, subdir))
                    if age < self.monitor[d]['age']:
                        self.monitor[d]['age']=age

                # optionally get age of all files
                if self.files:
                    for f in files:
                        age = timestamp - os.path.getmtime(os.path.join(root, f))
                        if age < self.monitor[d]['age']:
                            self.monitor[d]['age']=age                        

        # Sort with oldest first
        self.monitor.sort(key=lambda x: x['age'])

        # get dirs considered "static"
        self.static = [x for x in self.monitor if x['age'] > self.threshold ]

        retlist = []
        msglist = []
        if make_tombstones:
            for d in self.static:
                tombname = os.path.join(d['name'], self.filename)
                with open(tombname, 'w') as f:
                    retlist.append(tombname)

                    # Add notification here
                    msglist.append(self.make_obituary(tombname))
                    
        if len(msglist) > 0:
            for m in msglist:
                self.send_obituary(m)
                
        return(retlist)
    
def main():

    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description="Create tombstone files in static directories")
    parser.add_argument("--path", '-p', type=str, required=True, help="Location to save dicom images") 
    parser.add_argument("--level", '-l', type=int, help="Number of levels down to monitor", default=1)
    parser.add_argument("--depth", '-d', type=int, help="How many level deep with a dir to monitor to changes to subdirs", default=0)
    parser.add_argument("--static", '-s', type=float, help="How many seconds to be considered static", required=True)
    parser.add_argument("--tombstone", '-t', type=str, help="Filename for tombstone file", default=None)
    parser.add_argument("--files", '-f', action='store_true', help="Check all file mtimes", default=False)
    parser.add_argument("--info", '-i', action='store_true', help="List info but don't create tombstones")
    parser.add_argument("--wait", '-w', type=int, help="Time (s) between scans for static directories", default=None)
    parser.add_argument("--logging", '-g', type=str, help="Filename for logging output", default=None)
    parser.add_argument("--queue", '-q', type=str, nargs=3, help="RabbitMQ info: IP port queue_name", default=None)
    parser.add_argument("--verbose", '-v', action='store_true', help="Verbose output")
    args = parser.parse_args()            

    # Setup logging
    logger = logging.getLogger("tombstone")
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    if args.logging is not None:
        fileHandler = logging.FileHandler(args.logging)    
        fileHandler.setFormatter(formatter)
        logger.addHandler(fileHandler)

    # Setup parameters
    t = Tombstone(args.path, args.level, args.depth, args.static)
    t.filename = args.tombstone
    t.files = args.files
    t.queue = args.queue
    make_tombstones = (args.tombstone is not None) and (not args.info)
    scanning=True

    # Log startup
    if logger is not None:
        logger.info("Startup")
        
        if args.wait is not None:
            logger.info("Scanning frequency: "+str(args.wait)+'s')

    # Scan directories
    n_monitor=-1
    while scanning:

        scanning = args.wait is not None
        tlist = t.update(make_tombstones)

        if logger is not None:
            for f in tlist:
                logger.info("Created tombstone: "+f)

        # Extra logging info
        if args.verbose:
            if len(t.monitor) != n_monitor:
                logger.debug("Monitoring "+str(len(t.monitor))+" directories")
            n_monitor = len(t.monitor)

        # Pause before rescan
        if scanning:
            time.sleep(args.wait)


if __name__ == "__main__":
    main()

