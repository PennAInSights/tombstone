import pytest
from tombstone import Tombstone
import tempfile
import os
import time
import shutil

def test_tombstone():

    # testing dir struct
    #.           c
    #.       a - 
    #.           d
    #. dir -      
    #.           e
    #.       b - 
    #.            f
    

    dir = tempfile.TemporaryDirectory()
    dir_a = os.path.join(dir.name, 'a')
    dir_b = os.path.join(dir.name, 'b')
    dir_c = os.path.join(dir_a, 'c')
    dir_d = os.path.join(dir_a, 'd')
    dir_e = os.path.join(dir_b, 'e')
    dir_f = os.path.join(dir_b, 'f')

    os.makedirs(dir_b)
    os.makedirs(dir_a)

    os.makedirs(dir_c)
    os.makedirs(dir_d)

    t1 = Tombstone(dir.name,1,1,0.1)
    x = t1.update(False)
    assert os.path.basename(t1.monitor[0]['name']) == 'a'

    time.sleep(3)

    os.makedirs(dir_e)
    os.makedirs(dir_f)

    t2 = Tombstone(dir.name,1,2,2.9)
    x = t2.update(False)

    shutil.rmtree(dir.name)

    assert os.path.basename(t2.monitor[0]['name']) == 'b'

if __name__ == "__main__":
    pytest.main()