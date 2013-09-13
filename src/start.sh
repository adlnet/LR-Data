#! /bin/sh
celeryd-multi start 6 -l INFO -Q:1 harvest -Q:2 validate -Q:3,4 image -Q save
