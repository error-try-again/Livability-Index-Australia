import {QueryArguments} from "../../interface/query-arguments-interface";
import {aggregateAverages} from "./aggregate-averages";

export const getAverageTemperature = async (arguments_: QueryArguments) => {
  const result = await aggregateAverages(arguments_, "main.temp");
  return result && result.temp ? {
    averageTemperature: result.temp,
    count: result.count
  } : { averageTemperature: 0, count: 0 };
};
