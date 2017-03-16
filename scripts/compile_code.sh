#!/usr/bin/env bash
cd ..
rm -rf ./iotingestion-1.0
echo "-------------------------------------------"
echo "             Compiling Code"
echo "-------------------------------------------"
sbt clean compile package universal:package-bin

mv ./target/universal/iotingestion-1.0.zip ./

unzip ./iotingestion-1.0.zip

rm ./iotingestion-1.0.zip

mv ./target/scala-2.11/iotingestion_2.11-1.0.jar ./iotingestion-1.0
echo "------------------------------------------------------------------------------------------------"
echo "     Code compiled at the forder $PWD/iotingestion-1.0"
echo "------------------------------------------------------------------------------------------------"
