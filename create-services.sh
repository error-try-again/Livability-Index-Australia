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
  npm install --save-dev typescript @types/node @apollo/server mongodb graphql @types/graphql graphql-tag dotenv joi ts-node-dev nodemon
}

create_server_configuration_files() {
  cat >.env <<-EOL
MONGO_URI=mongodb://root:example@mongo:27017/recordsDB
PORT=4000
EOL

  cat >tsconfig.json <<-EOL
{
  "ts-node": {
    "esm": true
  },
  "compilerOptions": {
      "rootDirs": ["src"],
      "outDir": "dist",
      "lib": ["es2020"],
      "target": "es2020",
      "module": "ESNext",
      "target": "ESNext",
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
    "start": "npm run compile && nodemon --exec node --loader ts-node/esm src/index.ts",
    "compile": "tsc"
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
# Specify node version
FROM node:20.7.0

# Specify work directory
WORKDIR /usr/app

# Copy over package.json and package-lock.json
COPY package*.json ./
RUN npm install

# Copy over other source files
COPY . .

# Expose the port the app runs on
EXPOSE 4000

# Command to run on container start
CMD [ "npm", "start" ]
EOL
}

create_server_source_files() {

  cat >src/index.ts <<-'EOL'
import 'dotenv/config';
import gql from 'graphql-tag';
import { ApolloServer } from '@apollo/server';
import { startStandaloneServer } from '@apollo/server/standalone';
import { MongoClient, Db } from 'mongodb';

// Constants
const { MONGO_URI = '', DB_NAME = 'recordsDB', PORT: PORT_ENV } = process.env;
const PORT = parseInt(PORT_ENV, 10) || 4000;
const DEFAULT_LIMIT = 10;

// Database Variables
let client: MongoClient;
let db: Db;

// Database Connection Functions
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
    } catch (error) {
        console.error("Error disconnecting from MongoDB:", error);
    }
}

// Utility Functions
const buildFilter = (args: QueryArgs): object => {
    const filter: any = {};
    if (args.startDate) filter.dt_iso = { $gte: new Date(args.startDate).toISOString() };
    if (args.endDate) filter.dt_iso = { $lte: new Date(args.endDate).toISOString() };
    return filter;
};

// Data Fetching Functions
const fetchCities = async () => {
    const collections = await db.listCollections().toArray();
    return collections.map(collection => collection.name);
}

// Data Fetching Functions
const fetchWeatherData = async (args: QueryArgs) => {
    try {
        const filter = buildFilter(args);
        return await db.collection(args.city)
            .find(filter)
            .limit(args.limit || DEFAULT_LIMIT)
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

const aggregateAverage = async (args: QueryArgs, field: string) => {
    const filter = buildFilter(args);

    // Extract the last component of the field for naming (e.g., "main.feels_like" -> "feels_like")
    const fieldName = field.split('.').pop();

    // Create the aggregation field name
    const aggregationFieldName = `avg${fieldName.charAt(0).toUpperCase() + fieldName.slice(1)}`;

    const aggregationField = {};
    aggregationField[aggregationFieldName] = { $avg: `$${field}` };

    const data = await db.collection(args.city)
        .aggregate([
            { $match: filter },
            {
                $group: {
                    _id: null,
                    ...aggregationField,
                    count: { $sum: 1 }
                }
            }
        ])
        .toArray();

    return data.length > 0 ? { [fieldName]: data[0][aggregationFieldName], count: data[0].count } : null;
};

const getAverageTemperature = async (args: QueryArgs) => {
    const result = await aggregateAverage(args, "main.temp");
    if (result && result.temp) {
        return { averageTemperature: result.temp, count: result.count };
    } else {
        return { averageTemperature: 0, count: 0 }; // Default values
    }
};

const getAverageFeelsLike = async (args: QueryArgs) => {
    const result = await aggregateAverage(args, "main.feels_like");
    if (result && result.feels_like) {
        return { averageFeelsLike: result.feels_like, count: result.count };
    } else {
        return { averageFeelsLike: 0, count: 0 }; // Default values
    }
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

    type AverageTemperature {
        averageTemperature: Float!
        count: Int!
    }

    type AverageFeelsLike {
        averageFeelsLike: Float!
        count: Int!
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
        getAllCities: [String!]!

        getWeather(
            city: String!,
            startDate: String,
            endDate: String,
            limit: Int = 10,
            skip: Int = 0
        ): [Weather!]!

        getAverageTemperature(
            city: String!,
            startDate: String,
            endDate: String
        ): AverageTemperature!

        getAverageFeelsLike(
            city: String!,
            startDate: String,
            endDate: String
        ): AverageFeelsLike!
    }
`;

interface QueryArgs {
    city: string;
    season?: string;
    limit?: number;
    skip?: number;
    startDate?: string;
    endDate?: string;
};

const resolvers = {
    Query: {
        getAllCities: fetchCities,
        getWeather: (_, args: QueryArgs) => fetchWeatherData(args),
        getAverageTemperature: (_, args: QueryArgs) => getAverageTemperature(args),
        getAverageFeelsLike: (_, args: QueryArgs) => getAverageFeelsLike(args),
    },
};


// Server Initialization
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
    volumes:
      - ~/graphql-server/:/usr/app/
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
      docker exec graphql-server-mongo-1 mongosh --quiet --eval "disableTelemetry()"
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
