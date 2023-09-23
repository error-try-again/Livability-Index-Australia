# Livability-Index-Australia
The project aims to build an open, fully scalable, automated, and self-hostable independent livability index system per city based on historical and live data from several Australian government and commercial sources. 

The project has several aspects. 

1. Custom containers operate in a rootless dockerized environment (MongoDB + Apollo) and communicate via published ports. Apollo depends on the MongoDB service, and the exposed environment variable 'MONGO_URI' allows the database connection string to be available to Apollo on server start.

2. On the first run, the data/automation layer handles dataset injection into the database into a specific collection. 

3. Data retrieval, Modelling, and pagination via Apollo/GraphQL API Gateway.

# RoadMap
- [ ] Automation via crond scripts
- [ ] More datasets
- [ ] Code generation for frontend
- [ ] More resolvers 
- [ ] Additional typeDefs 
- [ ] Aggregate queries on mongodb for faster data processing
- [ ] Mongodb indexing and sharding
- [ ] SSL termination 
