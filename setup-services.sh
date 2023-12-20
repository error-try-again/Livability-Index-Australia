#!/bin/bash

set -euo pipefail
trap 'echo "Error: $BASH_SOURCE:$LINENO"' ERR

# --------- Configurations ---------
APP_ID=""
DOCKERD_ROOTLESS_SETUP_TOOL="dockerd-rootless-setuptool.sh"
CITIES_DATA_DIRECTORY="assets/cities"
OPEN_WEATHER_API_ENDPOINT="https://api.openweathermap.org/data/2.5/weather"
MONGO_URI="mongodb://root:example@mongo:27017/recordsDB"
DOCKER_COMPOSE_FILE="docker-compose.yml"
MONGO_DOCKER_CONTAINER="livability-index-australia-mongo-1"
APOLLO_DOCKER_CONTAINER="livability-index-australia-apollo-1"

# --------- Helper Functions ---------

check_required_cmds() {
  local cmd
  for cmd in jq curl docker; do
    if ! command -v "$cmd" &>/dev/null; then
      echo "Error: Required command '$cmd' is not installed."
      exit 1
    fi
  done
}

read_city_names() {
  jq -r '.locations[].name' "$CITIES_DATA_DIRECTORY/city_names.json"
}

check_and_create_directory() {
  local dir="$1"
  if [[ ! -d "$dir" ]]; then
    echo "Creating directory: $dir"
    mkdir -p "$dir"
  fi
  if [[ ! -w "$dir" ]]; then
    echo "Error: Directory $dir is not writable."
    exit 1
  fi
}

fetch_and_save_weather_data() {
  local city="$1"
  local weather_file="$CITIES_DATA_DIRECTORY/$city.json"
  echo "Fetching weather data for $city..."
  curl -s "$OPEN_WEATHER_API_ENDPOINT?q=$city&appid=$APP_ID" | jq '[.]' > "$weather_file"
}

# --------- Docker Setup Functions ---------

setup_docker_rootless() {
  echo "Setting up Docker in rootless mode..."

  if ! command -v "$DOCKERD_ROOTLESS_SETUP_TOOL" &>/dev/null; then
    $DOCKERD_ROOTLESS_SETUP_TOOL install
    {
      echo "export PATH=/home/docker-primary/bin:$PATH"
      echo "export DOCKER_HOST=unix:///run/user/$(id -u)/docker.sock"
      echo "export XDG_RUNTIME_DIR=/run/user/$(id -u)"
    } >>~/.bashrc
  else
    echo "Docker rootless mode is already set up."
  fi
}

# --------- City Data Setup Functions ---------

setup_city_data_files() {
  check_and_create_directory "$CITIES_DATA_DIRECTORY"
  for location in $(read_city_names); do
    fetch_and_save_weather_data "$location"
  done
}

import_city_data_to_mongo() {
  for location in $(read_city_names); do
    local json_file="$CITIES_DATA_DIRECTORY/${location}.json"
    if [[ -f "$json_file" ]]; then
      echo "Importing data for $location..."
      docker exec -i "$MONGO_DOCKER_CONTAINER" mongoimport --uri="$MONGO_URI" \
        --authenticationDatabase=admin --collection="$location" --file="$json_file" --jsonArray
    else
      echo "Warning: JSON file for $location not found, skipping import."
    fi
  done
}

# --------- Docker Environment Setup Functions ---------

create_docker_compose_file() {
  cat >"$DOCKER_COMPOSE_FILE" <<-EOL
version: '3.8'
services:
  mongo:
    build:
      context: ./mongodb
      dockerfile: Dockerfile
    ports:
      - "27017:27017"
    volumes:
      - mongodb-data:/data/db
      - ./assets/cities:/data/db/assets/cities
    environment:
      - MONGO_INITDB_ROOT_USERNAME=root
      - MONGO_INITDB_ROOT_PASSWORD=example
      - MONGO_INITDB_DATABASE=recordsDB

  apollo:
    build:
      context: ./apollo
      dockerfile: Dockerfile
    volumes:
      - ./apollo:/usr/app
    ports:
      - "4000:4000"
    depends_on:
      - mongo
    environment:
      - MONGO_URI=mongodb://root:example@mongo:27017/recordsDB?authSource=admin

volumes:
  mongodb-data:
  apollo-server:
EOL
}

run_docker_compose() {
  echo "Building Docker Compose..."
  docker compose build --no-cache || {
    echo "Failed to build Docker Compose"
    exit 1
  }

  echo "Running Docker Compose..."
  docker compose up -d --remove-orphans || {
    echo "Failed to run Docker Compose"
    exit 1
  }
}

wait_for_mongodb() {
  local i
  local retries
  retries=5

  for ((i = 1; i <= retries; i++)); do
    if docker exec $MONGO_DOCKER_CONTAINER mongosh --quiet --eval "db.version()" &>/dev/null; then
      docker exec $MONGO_DOCKER_CONTAINER mongosh --quiet --eval "disableTelemetry()"
      return 0
    fi
    echo "Waiting for MongoDB to start... ($i/$retries)"
    sleep 10
  done

  echo "MongoDB did not start after $retries attempts"
  exit 1
}

setup_docker_environment() {
  create_docker_compose_file
  run_docker_compose
  wait_for_mongodb
  import_city_data_to_mongo
}

# --------- Apollo and Dependencies Setup Functions ---------

stop_and_remove_container() {
  local container_name="$1"

  if docker ps -a | grep -q "$container_name"; then
    docker stop "$container_name" || {
      echo "Failed to stop $container_name container"
      exit 1
    }
    docker rm -f "$container_name" || {
      echo "Failed to remove $container_name container"
      exit 1
    }
  else
    echo "$container_name container not found."
  fi
}

cleanup_previous_installation() {
  local containers
  local key

  declare -A containers=(
    ["apollo"]="$APOLLO_DOCKER_CONTAINER"
    ["mongo"]="$MONGO_DOCKER_CONTAINER "
  )

  for key in "${!containers[@]}"; do
    stop_and_remove_container "${containers[$key]}"
  done

}

setup_apollo_and_deps() {
  echo "Setting up Apollo GraphQL server and its dependencies..."

  cleanup_previous_installation

  # Call the functions to set up server files and city data files
  setup_city_data_files

  # Set up Docker environment
  setup_docker_environment
}

# --------- Main Function ---------

main() {
  check_required_cmds
  setup_docker_rootless
  setup_apollo_and_deps
  echo "Setup completed successfully."
}

main
