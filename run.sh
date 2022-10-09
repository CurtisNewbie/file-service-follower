#!/bin/bash

./main profile='prod' configFile=/usr/src/file-service-follower/config/app-conf-prod.json >> /usr/src/file-service-follower/logs/file-service-follower.log 2>&1

