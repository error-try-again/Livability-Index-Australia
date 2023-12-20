import {Db, MongoClient} from "mongodb";
import { DB_NAME, MONGO_URI } from '../config';

let client: MongoClient;
let database: Db;

async function connectToDatabase() {
  try {
    client = new MongoClient(MONGO_URI);
    await client.connect();
    database = client.db(DB_NAME);
    console.log('Connected to MongoDB');
  } catch (error) {
    console.error("Error connecting to MongoDB:", error);
    throw error;
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

export {
  connectToDatabase,
  disconnectFromDatabase,
  client,
  database,
}
