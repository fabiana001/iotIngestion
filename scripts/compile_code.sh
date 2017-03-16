#!/usr/bin/env bash
cd ..
rm -rf ./iotIngestion
echo "-------------------------------------------"
echo "             Compiling Code"
echo "-------------------------------------------"
sbt clean compile package universal:package-bin
mkdir iotIngestion
mv ./target/scala-2.11/iotingestion_2.11-1.0.jar ./iotIngestion/
mv ./target/universal/iotingestion-1.0.zip ./iotIngestion/
unzip ./iotIngestion/iotingestion-1.0.zip -d ./iotIngestion/
rm ./iotIngestion/iotingestion-1.0.zip
echo "------------------------------------------------------------------------------------------------"
echo "     Code compiled at the forder $PWD/iotIngestion"
echo "------------------------------------------------------------------------------------------------"
