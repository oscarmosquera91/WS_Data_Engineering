from datetime import datetime
from partition_utils import get_block_number_in_window

cases = [
    ("21:00", 4, 21, 5),
    ("23:30", 4, 21, 5),
    ("02:00", 4, 21, 5),
    ("04:30", 4, 21, 5),
    # ("05:00", 4, 21, 5),
    # ("23:30", 4, 22, 6),
    # ("02:30", 4, 22, 6),
    # ("21:59", 4, 22, 6),
    # ("12:00", 6, 0, 0),
]

for now_s, blocks, start, end in cases:
    now = datetime.strptime(now_s, "%H:%M")
    now = now.replace(year=2025, month=9, day=24)
    print(now)
    blk = get_block_number_in_window(blocks, start, end, now=now)
    
    print(now_s, 'blocks', blocks, f'{start:02d}:00-{end:02d}:00', '->', blk)


# blk = get_block_number_in_window(4, 6, 18, datetime.now())
# print(blk)