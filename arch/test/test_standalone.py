# !/usr/bin/python
# -*- coding: UTF-8 -*-

"""
@company:UDAI
@author:tianjian
@file:test_standalone.py
@time:2022/03/12

"""
from arch.session import computing_session as session

if __name__ == '__main__':
    session.init("123")
    table = session.parallelize([1, 2, 3], include_key=False, partition=4)
    table = table.map(lambda x, y: (x + 2, y * 2))
    print(list(table.collect()))
