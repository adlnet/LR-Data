#! /bin/sh
celeryd-multi start 3 -l INFO -Q:1 harvest,image -Q:2 validate -Q save
