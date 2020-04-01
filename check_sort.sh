#!/bin/bash

./generate $1 'source'
echo array generated

time './sort' 'source'
echo array sorted

./check result 'source'

echo $?
