#!/bin/bash

# Exit on any error
set -e

setup_docker_rootless() {
    echo "Setting up Docker in rootless mode..."

    if ! command -v dockerd-rootless-setuptool.sh &> /dev/null; then
        dockerd-rootless-setuptool.sh install
        echo "export PATH=/home/docker-primary/bin:$PATH" >> ~/.bashrc
        echo "export DOCKER_HOST=unix:///run/user/$(id -u)/docker.sock" >> ~/.bashrc
        echo "export XDG_RUNTIME_DIR=/run/user/$(id -u)" >> ~/.bashrc
    else
        echo "Docker rootless mode is already set up."
    fi
}

prompt_for_overwrite() {
    echo "The 'graphql-server' directory already exists."
    read -p "Do you want to overwrite it? (yes/no): " choice

    shopt -s nocasematch
    case "$choice" in
        yes|y ) return 0 ;;
        no|n ) return 1 ;;
        * ) echo "Invalid choice. Exiting." ; exit 1 ;;
    esac
}

setup_server_files() {
    echo "Initializing NPM project..."
    npm init --yes && npm pkg set type="module"
    echo "Installing dependencies..."
    npm install --save-dev typescript @types/node @apollo/server mongodb graphql @types/graphql graphql-tag

        cat > tsconfig.json <<- EOL
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

        cat > package.json <<- EOL
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

cat > src/index.ts <<- 'EOL'
import gql from 'graphql-tag';

import { startStandaloneServer } from '@apollo/server/standalone';
import { ApolloServer } from '@apollo/server';
import { MongoClient } from 'mongodb';

const MONGO_URI = process.env.MONGO_URI || '';

async function startServer() {
    const client = new MongoClient(MONGO_URI);
    await client.connect();

    const typeDefs = gql`
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
        }

    `;

    const resolvers = {
        Query: {
            getWeatherByCity: async (_, { city, limit = 10, skip = 0, dt_iso}) => {
                const db = client.db('weatherDB');
                return await db.collection(city).find().limit(limit).skip(skip).toArray();
            },
        },
    };

    const server = new ApolloServer({
        typeDefs,
        resolvers,
    });

    const { url } = await startStandaloneServer(server, {
        listen: { port: 4000 },
    });

    console.log(`Server ready at: ${url}`);
}

startServer();
EOL

        cat > Dockerfile <<- EOL
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

setup_city_data() {

    mkdir -p cities .
    cp -R ~/cities/* cities/

    cat > cities/city_names.json <<- EOL
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

    # Define the MongoDB URI
    MONGO_URI="mongodb://root:example@mongo:27017/weatherDB"

    cities=($(jq -r '.cities[]' cities/city_names.json ))

    # Iterate over each city and import data
    for city in "${cities[@]}"
        do
        echo "Attempting to import data for $city"
        json_file="cities/${city}.json"
        docker exec -i graphql-server-mongo-1 mongoimport --uri="$MONGO_URI"  --authenticationDatabase=admin --collection="$city" --file="$json_file" --jsonArray
    done
}


setup_docker_environment() {
    cat > docker-compose.yml <<- EOL
version: '3.8'
services:
  mongo:
    image: mongodb/mongodb-community-server:latest
    ports:
      - "27017:27017"
    volumes:
      - mongodb_data:/data/db
      - ~/graphql-server/cities:/cities
    environment:
      - MONGO_INITDB_ROOT_USERNAME=root
      - MONGO_INITDB_ROOT_PASSWORD=example
      - MONGO_INITDB_DATABASE=weatherDB

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

    echo "Building Docker image..."
    docker build -t apollo . || { echo "Failed to build Docker image"; exit 1; }

    sleep 10;

    echo "Running Docker Compose..."
    docker compose up -d || { echo "Failed to run Docker Compose"; exit 1; }

    sleep 10;

    echo "exit" | docker exec -i graphql-server-mongo-1 mongosh

    sleep 10;

    setup_city_data

}

setup_apollo_and_deps() {
    echo "Setting up Apollo GraphQL server and its dependencies..."

    if [ -d "graphql-server" ] && ! prompt_for_overwrite; then
        echo "Skipping 'graphql-server' setup."
        return
    fi

    rm -rf graphql-server

    docker system prune -a

#     docker rm -f $(docker ps -a -q)

    mkdir -p graphql-server/src
    cd graphql-server || { echo "Failed to change directory to graphql-server"; exit 1; }

    setup_server_files
    setup_docker_environment

}

main() {
    setup_docker_rootless
    setup_apollo_and_deps

    echo "Setup completed successfully."
}

main
