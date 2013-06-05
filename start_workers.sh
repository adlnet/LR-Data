#!/bin/bash
for i in {1..10}
do
  exec python -m mincemeat -p password localhost
done 
