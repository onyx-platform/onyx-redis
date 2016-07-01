#!/bin/sh -x
docker run --name redis_container -p 6379:6379 -d redis
