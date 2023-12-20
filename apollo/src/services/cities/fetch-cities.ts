import {database} from "../../database/mongo-util";

const fetchCities = async () => {
  const collections = await database.listCollections().toArray();
  return collections.map(collection => {
    return collection.name;
  });
};

export {
  fetchCities,
}
