#!/bin/bash
build/mvn -DskipTests package > output.txt
grep "error" output.txt
grep "BUILD SUCCESS" output.txt
