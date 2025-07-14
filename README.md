# tombstone

tombstone is a simple class that examines subdirectories under a base directory at a specified level. The last modified time is determined for each subdirectory and if the subdirectory is older than the provided threshold, a "tombstone" file is added to mark the directory as static. Optionally, the mtime of files may also be considered. This is indended to help monitor directories that are receiving data but lack other criteria to determine when the transfer is complete.

## Features

- Set the desired level of subdirectories to monitor
- Set the depth at which subdirectories are probed to determine the time of most recent mtime
- Set the desired filename to be used as a tombstone
- Set the "age" at which a tombstone is created
- Optionally, also examine the mtime of files in monitored directories

## Installation

You can install the package via **PyPI** or from **source**

### Intsall from PyPI

```bash
pip install tombstone
```

### Install from Source (github)

```bash
git clone https://github.com/jeffduda/tombstone.git
cd tombstone
pip install .
```


## Usage 
After installation you can use 'Tombstone' to monitor subdirectories for most recent mtime

### Example

```python
from tombstone import Tombstone

t = Tombstone(directory="/My/Base/Dir",level=1,depth=2,threshold=60)
stones = t.update(True)

for s in stones:
    print(f"Created tombstone: {s}")
```