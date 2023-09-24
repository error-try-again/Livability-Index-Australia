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
import { ApolloServer } from '@apollo/server';
import { startStandaloneServer } from '@apollo/server/standalone';
import { MongoClient, Db } from 'mongodb';

const { MONGO_URI = '', DB_NAME = 'recordsDB', PORT: PORT_ENV } = process.env;
const PORT = parseInt(PORT_ENV, 10) || 4000;

let client: MongoClient;
let db: Db;

async function connectToDatabase() {
    try {
        client = await MongoClient.connect(MONGO_URI);
        db = client.db(DB_NAME);
        console.log('Connected to MongoDB');
    } catch (err) {
        console.error("Error connecting to MongoDB:", err);
        throw err;
    }
}

async function disconnectFromDatabase() {
    try {
        await client?.close();
        console.log('Disconnected from MongoDB');
    } catch (err) {
        console.error("Error disconnecting from MongoDB:", err);
    }
}

// Database helper functions
const buildFilter = (args: QueryArgs): object => {
    const filter: any = {};
    if (args && args.dt_iso) filter.dt_iso = args.dt_iso;
    if (args && args.startDate && args.endDate) filter.dt_iso = { $gte: args.startDate, $lte: args.endDate };
    return filter;
};


const fetchWeatherData = async (args: QueryArgs) => {
    try {
    const filter = buildFilter(args);

    return await db.collection(args.city)
        .find(filter)
        .limit(args.limit || 10)
        .skip(args.skip || 0)
        .map(item => {
            if (item.rain && item.rain['1h']) {
                item.rain.duration_1h = item.rain['1h'];
                delete item.rain['1h'];
            }
            return item;
        })
        .toArray();
     } catch (error) {
        console.error("Error fetching weather data:", error);
        throw error;
    }
};

const fetchCities = async () => {
    const collections = await db.listCollections().toArray();
    return collections.map(coll => coll.name);
}

const getAverageTemperatureOverYears = async (args: QueryArgs) => {
    const yearsAgo = new Date();
    yearsAgo.setFullYear(yearsAgo.getFullYear() - args.years);

    const data = await db.collection(args.city)
        .aggregate([
            { $match: { dt_iso: { $gte: yearsAgo.toISOString() } } },
            { $group: { _id: null, avgTemp: { $avg: "$main.temp" } } }
        ])
        .toArray();

    return data.length > 0 ? { averageTemperature: data[0].avgTemp } : null;
};

const getSeasonalAverageOverYears = async (args: QueryArgs) => {
    const yearsAgo = new Date();
    yearsAgo.setFullYear(yearsAgo.getFullYear() - args.years);

    return await db.collection(args.city)
        .aggregate([
            {
                $addFields: {
                    convertedDate: {
                        $dateFromString: {
                            dateString: "$dt_iso",
                            timezone: "UTC"
                        }
                    }
                }
            },
            { $match: { convertedDate: { $gte: yearsAgo } } },
            {
                $group: {
                    _id: {
                        season: {
                            $switch: {
                                branches: [
                                    { case: { $lte: [{ $dayOfYear: "$convertedDate" }, 80] }, then: "Summer" },
                                    { case: { $lte: [{ $dayOfYear: "$convertedDate" }, 172] }, then: "Autumn" },
                                    { case: { $lte: [{ $dayOfYear: "$convertedDate" }, 264] }, then: "Winter" },
                                    { case: { $lte: [{ $dayOfYear: "$convertedDate" }, 355] }, then: "Spring" },
                                ],
                                default: "Summer"
                            }
                        }
                    },
                    avgTemp: { $avg: "$main.temp" },
                }
            }
        ])
        .toArray();
};

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

type Stats {
    averageTemperature: Float!
}

type SeasonalStats {
    season: String!
    averageTemperature: Float!
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
    duration_1h: Float!
}

type Wind {
    speed: Float!
    deg: Int!
}

type Query {
    getWeather(city: String!, dt_iso: String, startDate: String, endDate: String, limit: Int = 10, skip: Int = 0): [Weather!]!
    getAllCities: [String!]!
    getAverageTemperatureOverYears(city: String!, years: Int = 0): Stats!
    getSeasonalAverageOverYears(city: String!, years: Int = 0): [SeasonalStats!]!
}
`;

interface QueryArgs {
    city: string;
    limit?: number;
    skip?: number;
    dt_iso?: string;
    startDate?: string;
    endDate?: string;
    years?: number;
}

const resolvers = {
    Query: {
        getWeather: (_, args: QueryArgs) => fetchWeatherData(args),
        getAllCities: fetchCities,
        getAverageTemperatureOverYears: (_, args: QueryArgs) => getAverageTemperatureOverYears(args),
        getSeasonalAverageOverYears: (_, args: QueryArgs) => getSeasonalAverageOverYears(args)
    },
};

async function startServer() {
    const typeDefs = [weatherTypeDefs];
    const server = new ApolloServer({ typeDefs, resolvers });

    const { url } = await startStandaloneServer(server, { listen: { port: PORT } });
    console.log(`Server ready at: ${url}`);

    ['SIGTERM', 'SIGINT'].forEach(signal => {
        process.on(signal as any, async () => {
            console.log(`${signal} signal received. Shutting down gracefully...`);
            await disconnectFromDatabase();
            process.exit(0);
        });
    });
}

async function main() {
    if (!MONGO_URI) {
        console.error("Please set MONGO_URI in your .env file.");
        process.exit(1);
    }

    try {
        await connectToDatabase();
        await startServer();
    } catch (error) {
        console.error("Error initializing the server:", error);
        await disconnectFromDatabase();
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
