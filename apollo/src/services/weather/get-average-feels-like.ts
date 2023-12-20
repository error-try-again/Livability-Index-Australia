import {aggregateAverages} from "./aggregate-averages";
import {QueryArguments} from "../../interface/query-arguments-interface";

export const getAverageFeelsLike = async (arguments_: QueryArguments) => {
  const result = await aggregateAverages(arguments_, "main.feels_like");
  return result && result.feels_like ? {
    averageFeelsLike: result.feels_like,
    count: result.count
  } : { averageFeelsLike: 0, count: 0 };
};
