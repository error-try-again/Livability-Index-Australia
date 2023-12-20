const DEFAULT_LIMIT = 10;
const PORT_ENV: string = process.env.PORT || '4000';
const PORT = Number.parseInt(PORT_ENV, 10) || 4000;
const MONGO_URI: string = process.env.MONGO_URI || '';
const DB_NAME = 'recordsDB' || process.env.DB_NAME;

export {
  DEFAULT_LIMIT,
  PORT,
  MONGO_URI,
  DB_NAME
};
