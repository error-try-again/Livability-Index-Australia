import 'dotenv/config';
import {ApolloServer} from '@apollo/server';
import {startStandaloneServer} from '@apollo/server/standalone';
import {connectToDatabase, disconnectFromDatabase} from "./database/mongo-util";
import {resolvers} from "./resolvers/query-resolvers";
import { MONGO_URI, PORT } from './config';
import { weatherTypeDefinitions } from './graph-ql/weather-type-definitions';

async function startServer() {
  const typeDefs = [weatherTypeDefinitions];
  const server = new ApolloServer({resolvers, typeDefs});
  const {url} = await startStandaloneServer(server, {listen: {port: PORT}});
  for (const signal of ['SIGTERM', 'SIGINT']) {
    console.log(`Server running at ${url}`);
    process.on(signal, async () => {
      console.log(`${signal} signal received. Shutting down ${url}`);
      await disconnectFromDatabase();
      process.exit(0);
    });
  }
}

async function main() {
  if (!MONGO_URI) {
    console.error("Please set MONGO_URI in your .env file.");
    throw new Error("MONGO_URI not set");
  }

  try {
    await connectToDatabase();
    await startServer();
  } catch (error) {
    console.error("Error initializing the server:", error);
    await disconnectFromDatabase();
    throw error;
  }
}

await main();
