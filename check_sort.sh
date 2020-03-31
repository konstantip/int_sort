#!/bin/bash

./generate $1 'source'
echo array generated

'./sort' 'source'
echo array sorted

./check result 'source'

echo $?
