import {getAverageTemperature} from "../services/weather/get-average-temporary";
import {getAverageFeelsLike} from "../services/weather/get-average-feels-like";
import {fetchWeatherData} from "../services/weather/fetch-weather-data";
import {ResolveQueryArguments} from "../interface/query-arguments-interface";
import { fetchCities } from '../services/cities/fetch-cities';

export const resolvers = {
  Query: {
    getAllCities: fetchCities,
    getAverageFeelsLike: ({args}: ResolveQueryArguments) => getAverageFeelsLike(args),
    getAverageTemperature({args}: ResolveQueryArguments) {
      return getAverageTemperature(args);
    },
    getWeather({args}: ResolveQueryArguments) {
      return fetchWeatherData(args);
    },
  },
};
