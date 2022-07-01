#!/bin/bash
set -e
cd "$(dirname "$0")"
PBF_FILE=data/berlin-latest.osm.pbf
IMAGE=osrm/osrm-backend:latest
if [[ "$(docker images -q ${IMAGE} 2> /dev/null)" == "" ]]; then
  echo "pulling docker image"
  docker pull osrm/osrm-backend
else
  echo "image already exists"
fi
mkdir -p data/cache
if [ -f "${PBF_FILE}" ]; then
  echo "PBF file already exists"
else
  echo "downloading pbf file of berlin"
  curl http://download.geofabrik.de/europe/germany/berlin-latest.osm.pbf --output ${PBF_FILE}
fi
if [ -f "data/berlin-latest.osrm" ]; then
    echo "osm file already loaded"
else
    docker run -t -v "${PWD}/data:/data" osrm/osrm-backend osrm-extract -p /opt/bicycle.lua /data/berlin-latest.osm.pbf
    docker run -t -v "${PWD}/data:/data" osrm/osrm-backend osrm-partition /data/berlin-latest.osrm
    docker run -t -v "${PWD}/data:/data" osrm/osrm-backend osrm-customize /data/berlin-latest.osrm
fi
echo "running http server at port 5000."
docker run -t -i -p 5000:5000 -v "${PWD}/data:/data" osrm/osrm-backend osrm-routed --algorithm mld /data/berlin-latest.osrm