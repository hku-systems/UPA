#!/bin/bash
build/mvn -DskipTests -T 40 package > output.txt

grep "error" output.txt
grep "BUILD SUCCESS" output.txt
