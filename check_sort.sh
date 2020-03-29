#!/bin/bash

./generate $1 'source'

'./sort' 'source'

./check result 'source'

echo $?
