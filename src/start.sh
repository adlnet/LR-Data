#! /bin/sh
celeryd-multi start 4 -l INFO -Q:1 harvest,validate -Q:2 image -Q save
