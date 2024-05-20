#!/usr/bin/env bash

mvn compile exec:java -Dexec.mainClass="website.cafebabe.CreateMeasurements" -Dexec.args="1000000000"