import { QueryArguments } from '../interface/query-arguments-interface';

interface Filter {
  dt_iso: Date | { $gte?: string; $lte?: string };
}

export const buildFilter = (arguments_: QueryArguments): Filter => {
  const filter: Filter = {} as Filter;

  // Create a string key based on the presence of startDate and endDate
  const datePresenceKey = `${arguments_.startDate ? 'start' : ''}${arguments_.endDate ? 'end' : ''}`;

  switch (datePresenceKey) {
    // Both startDate and endDate are present
    case 'startend': {
      filter.dt_iso = {
        $gte: new Date(arguments_.startDate!).toISOString(),
        $lte: new Date(arguments_.endDate!).toISOString()
      };
      break;
    }
    // Only startDate is present
    case 'start': {
      filter.dt_iso = { $gte: new Date(arguments_.startDate!).toISOString() };
      break;
    }
    // Only endDate is present
    case 'end': {
      filter.dt_iso = { $lte: new Date(arguments_.endDate!).toISOString() };
      break;
    }
    // Neither startDate nor endDate is present
    default: {
      filter.dt_iso = new Date();
      break;
    }
  }

  return filter;
};
