import { gql } from 'graphql-tag';

const weatherTypeDefinitions = gql`
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

export {
  weatherTypeDefinitions
};
