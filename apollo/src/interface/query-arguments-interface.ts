interface QueryArguments {
  city: string;
  season?: string;
  limit?: number;
  skip?: number;
  startDate?: string;
  endDate?: string;
}

interface ResolveQueryArguments {
  args: QueryArguments;
}

export type {
  QueryArguments,
  ResolveQueryArguments
}
