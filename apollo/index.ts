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
  if (args.startDate) {
    filter.dt_iso = {$gte: new Date(args.startDate).toISOString()};
  }
  if (args.endDate) {
    filter.dt_iso = {$lte: new Date(args.endDate).toISOString()};
  }
  return filter;
};

// Data Fetching Functions
const fetchCities = async () => {
  const collections = await db.listCollections().toArray();
  return collections.map(collection => collection.name);
};

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
}

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

main().then(r => console.log("Server started successfully: " + r));
