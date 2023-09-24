#!/bin/bash

# Exit on any error
set -e

# --------- Docker Setup Functions ---------

setup_docker_rootless() {
  echo "Setting up Docker in rootless mode..."

  if ! command -v dockerd-rootless-setuptool.sh &>/dev/null; then
    dockerd-rootless-setuptool.sh install
    echo "export PATH=/home/docker-primary/bin:$PATH" >>~/.bashrc
    echo "export DOCKER_HOST=unix:///run/user/$(id -u)/docker.sock" >>~/.bashrc
    echo "export XDG_RUNTIME_DIR=/run/user/$(id -u)" >>~/.bashrc
  else
    echo "Docker rootless mode is already set up."
  fi
}

# --------- Apollo GraphQL Server Setup Functions ---------

prompt_for_overwrite() {
  echo "The 'graphql-server' directory already exists."
  read -p "Do you want to overwrite it? (yes/no): " choice

  shopt -s nocasematch
  case "$choice" in
  yes | y) return 0 ;;
  no | n) return 1 ;;
  *)
    echo "Invalid choice. Exiting."
    exit 1
    ;;
  esac
}

initialize_server_project() {
  echo "Initializing NPM project..."
  npm init --yes && npm pkg set type="module"
  echo "Installing dependencies..."
  npm install --save-dev typescript @types/node @apollo/server mongodb graphql @types/graphql graphql-tag dotenv joi
}

create_server_configuration_files() {
  cat >.env <<-EOL
MONGO_URI=mongodb://root:example@mongo:27017/recordsDB
PORT=4000
EOL

  cat >tsconfig.json <<-EOL
{
"compilerOptions": {
    "rootDirs": ["src"],
    "outDir": "dist",
    "lib": ["es2020"],
    "target": "es2020",
    "module": "esnext",
    "moduleResolution": "node",
    "esModuleInterop": true,
    "types": ["node"]
}
}
EOL

  cat >package.json <<-EOL
{
  "type": "module",
  "scripts": {
      "compile": "tsc",
      "start": "npm run compile && node ./dist/index.js"
  },
  "dependencies": {
      "@apollo/server": "^4.9.3",
      "graphql": "^16.8.1",
      "mongodb": "^4.2.0"
  },
  "devDependencies": {
      "@types/node": "^20.6.3",
      "typescript": "^5.2.2"
  }
}
EOL


  cat >Dockerfile <<-EOL
FROM node:20.7.0

WORKDIR /usr/app

COPY package*.json ./
RUN npm install

COPY . .

RUN npm run compile

EXPOSE 4000
CMD [ "node", "./dist/index.js" ]
EOL
}

create_server_source_files() {

  cat >src/index.ts <<-'EOL'
import 'dotenv/config';
import gql from 'graphql-tag';
import { startStandaloneServer } from '@apollo/server/standalone';
import { ApolloServer } from '@apollo/server';
import { MongoClient, Db } from 'mongodb';

const MONGO_URI = process.env.MONGO_URI;
const PORT = parseInt(process.env.PORT, 10) || 4000;

let client: MongoClient;
let db: Db;

const state = {
    db: null,
    mode: null
};

async function connectToDatabase(): Promise<Db> {
    if (state.db) {
        console.log('Already connected to MongoDB');
        return state.db;
    }

    try {
        client = await MongoClient.connect(MONGO_URI);
        state.db = client.db('recordsDB');
        state.mode = 'connected';
        console.log('Connected to MongoDB');
        return state.db;
    } catch (error) {
        console.error("Failed to connect to MongoDB:", error);
        throw error;
    }
}


function getDatabase() {
    if (!state.db) {
        throw new Error('Database is not initialized. Call connectToDatabase first.');
    }
    return state.db;
}

async function disconnectFromDatabase() {
    if (client) {
        await client.close();
    }
}

// GraphQL Typedefs
export const weatherTypeDefs = gql`
type Weather {
    dt: Int!
    dt_iso: String!
    timezone: Int!
    main: Main!
    clouds: Clouds!
    weather: [WeatherInfo!]!
    rain: Rain
    wind: Wind!
    lat: Float!
    lon: Float!
    city_name: String!
}

type Main {
    temp: Float!
    temp_min: Float!
    temp_max: Float!
    feels_like: Float!
    pressure: Int!
    humidity: Int!
    dew_point: Float!
}

type Clouds {
    all: Int!
}

type WeatherInfo {
    id: Int!
    main: String!
    description: String!
    icon: String!
}

type Rain {
    hour: Float!
}

type Wind {
    speed: Float!
    deg: Int!
}

type Query {
    getWeatherByCity(city: String!, limit: Int, skip: Int): [Weather!]!
    getWeatherByCityDate(city: String!, limit: Int, skip: Int, dt_iso: String): [Weather!]!
}
`;

// GraphQL Resolvers
const resolvers = {
    Query: {
      getWeatherByCity: async (_, { city, limit = 10, skip = 0 }) => {
            const db = getDatabase();
            console.log(db);
            let filter: { [key: string]: any } = {};

            return await db.collection(city).find().limit(limit).skip(skip).toArray();
        },
        getWeatherByCityDate: async (_, { city, limit = 10, skip = 0, dt_iso }) => {
            const db = getDatabase();
            console.log(db);
            let filter: { [key: string]: any } = {};
            if (dt_iso) {
                filter.dt_iso = dt_iso;
            }
            return await db.collection(city).find(filter).limit(limit).skip(skip).toArray();
        },

    },
};

// Server logic
async function startServer() {
    const typeDefs = [weatherTypeDefs];
    const server = new ApolloServer({ typeDefs, resolvers });

    try {
        const { url } = await startStandaloneServer(server, { listen: { port: PORT } });
        console.log(`Server ready at: ${url}`);

        // Graceful shutdown
        ['SIGTERM', 'SIGINT'].forEach(signal => {
            process.on(signal as any, async () => {
                console.log(`${signal} signal received. Shutting down gracefully...`);
                await disconnectFromDatabase();
                process.exit(0);
            });
        });

    } catch (error) {
        console.error("Error starting server:", error);
    }
}

async function main() {
    try {
        await connectToDatabase();
        await startServer();
    } catch (error) {
        console.error("Error:", error);
        process.exit(1);
    }
}

main();
EOL
}

setup_server_files() {
  initialize_server_project
  create_server_configuration_files
  create_server_source_files
}

# --------- City Data Setup Functions ---------

setup_city_data_files() {
  mkdir -p cities .
  cp -R ~/cities/* cities/

  cat >cities/city_names.json <<-EOL
{
"cities": [
    "Brisbane",
    "Darwin",
    "Hobart",
    "Adelaide",
    "Melbourne",
    "Sydney",
    "Perth"
]
}
EOL
}

import_city_data_to_mongo() {
  local MONGO_URI="mongodb://root:example@mongo:27017/recordsDB"
  local cities=($(jq -r '.cities[]' cities/city_names.json))

  for city in "${cities[@]}"; do
    echo "Attempting to import data for $city"
    local json_file="cities/${city}.json"
    docker exec -i graphql-server-mongo-1 mongoimport --uri="$MONGO_URI" --authenticationDatabase=admin --collection="$city" --file="$json_file" --jsonArray
  done
}

# --------- Docker Environment Setup Functions ---------

create_docker_compose_file() {
  cat >docker-compose.yml <<-EOL
version: '3.8'
services:
  mongo:
    image: mongo:latest
    ports:
      - "27017:27017"
    volumes:
      - mongodb_data:/data/db
      - ~/graphql-server/cities:/cities
    environment:
      - MONGO_INITDB_ROOT_USERNAME=root
      - MONGO_INITDB_ROOT_PASSWORD=example
      - MONGO_INITDB_DATABASE=recordsDB

  apollo-server:
    build:
      context: ./
      dockerfile: Dockerfile
    ports:
      - "4000:4000"
    depends_on:
      - mongo
    environment:
      - MONGO_URI=mongodb://root:example@mongo:27017

volumes:
  mongodb_data:
EOL
}

build_docker_image() {
  echo "Building Docker image..."
  docker build -t apollo . || {
    echo "Failed to build Docker image"
    exit 1
  }
}

sleep 10

run_docker_compose() {
  echo "Running Docker Compose..."
  docker compose up -d || {
    echo "Failed to run Docker Compose"
    exit 1
  }
}

wait_for_mongodb() {
  local retries=5

  for ((i = 1; i <= $retries; i++)); do
    if docker exec graphql-server-mongo-1 mongosh --quiet --eval "db.version()" &>/dev/null; then
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
  build_docker_image
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
  declare -A containers=(
    ["apollo"]="graphql-server-apollo-server-1"
    ["mongo"]="graphql-server-mongo-1"
  )

  for key in "${!containers[@]}"; do
    stop_and_remove_container "${containers[$key]}"
  done

  rm -rf graphql-server
}

setup_apollo_and_deps() {
  echo "Setting up Apollo GraphQL server and its dependencies..."

  if [ -d "graphql-server" ] && ! prompt_for_overwrite; then
    echo "Skipping 'graphql-server' setup."
    return
  fi

  cleanup_previous_installation

  mkdir -p graphql-server/src
  cd graphql-server

  setup_server_files
  setup_city_data_files
  setup_docker_environment
}

# --------- Main Function ---------

main() {
  setup_docker_rootless
  setup_apollo_and_deps

  docker container ls | grep graphql-server
  echo "Setup completed successfully."
}

main
