#!/usr/bin/python
# coding:utf-8

# @FileName:    main.py
# @Time:        2025/11/11 21:50
# @Author:      bubu
# @Project:     douyinLiveWebFetcher

from liveMan import DouyinLiveWebFetcher

if __name__ == '__main__':
    # live_id = '414750639705' #
    live_id = '447840496489' #龙
    room = DouyinLiveWebFetcher(live_id)
    # room.get_room_status() # 失效

    room.start()