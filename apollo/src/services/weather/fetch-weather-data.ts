
import {buildFilter} from "../../utility/build-filter";
import {database} from "../../database/mongo-util";
import {DEFAULT_LIMIT} from "../../config";
import {QueryArguments} from "../../interface/query-arguments-interface";

export const fetchWeatherData = async (arguments_: QueryArguments) => {
    const filter = buildFilter(arguments_);
    return await database.collection(arguments_.city)
    .find(filter)
    .limit(arguments_.limit || DEFAULT_LIMIT)
    .skip(arguments_.skip || 0)
    .map(item => {
      if (item.rain && item.rain['1h']) {
        item.rain.duration_1h = item.rain['1h'];
        delete item.rain['1h'];
      }
      return item;
    })
    .toArray();
};
