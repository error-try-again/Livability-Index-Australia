import { buildFilter } from '../../utility/build-filter';
import { database } from '../../database/mongo-util';
import { QueryArguments } from '../../interface/query-arguments-interface';

export const aggregateAverages = async (arguments_: QueryArguments, field: string) => {
  if (!field) {
    console.log('Field is undefined');
    return;
  }

  const filter = buildFilter(arguments_);
  const fieldName = field.split('.').pop();

  // Guard clause for fieldName
  if (!fieldName) {
    console.log('Invalid field format');
    return;
  }

  const aggregationFieldName = `avg${fieldName.charAt(0).toUpperCase() + fieldName.slice(1)}`;
  const aggregationField = { [aggregationFieldName]: { $avg: `$${field}` } };

  try {
    const [data] = await database.collection(arguments_.city)
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

    // Utilizing optional chaining to simplify the return statement
    return data ? {
      [fieldName]: data[aggregationFieldName],
      count: data.count
    } : null;
  } catch (error) {
    console.error('Error in aggregateAverages:', error);
    throw error; // Rethrow to allow caller to handle the error
  }
};
