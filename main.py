#!/usr/bin/python
# coding:utf-8

# @FileName:    main.py
# @Time:        2025/11/11 21:50
# @Author:      bubu
# @Project:     douyinLiveWebFetcher

from liveMan import DouyinLiveWebFetcher

if __name__ == '__main__':
    live_id = '447840496489' 
    room = DouyinLiveWebFetcher(live_id)


    room.start()
