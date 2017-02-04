import time
from itertools import repeat
from pprint import pprint

from steem import Steem

s = Steem()

block_num = s.rpc.get_dynamic_global_properties()['head_block_number']
print(block_num)

for _ in repeat(None):
    pprint(s.rpc.get_block(block_num))
    time.sleep(3)
