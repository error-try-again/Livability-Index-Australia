# Livability-Index-Australia
The project aims to build an open, fully scalable, automated, and self-hostable independent livability index system per city based on historical and live data from several Australian government and commercial sources. 
By representing complex data for weather, traffic, emergency services, rentals, education, and others to distill clearly what it might be like to live in one of Australia's major cities.        

# Why? 
How many times have you had conversations with people waxing poetic about what it must be like to live somewhere else? I know I have. It's my personal belief that it's unreasonable to assume via anecdotal or singular sources what it's like to live in a specific place without living there yourself...
Or an enormous amount of relevant data.

# Important notes:
*setup.sh uses a default password for docker-primary, so after running setup.sh you need to change it, especially if you're working on a remote system.*

For bonus points, just copy your public key over and disable password logins altogether. 

`ssh-copy-id -i .ssh/my-key.pub my-primary-user@ip`

`ssh-copy-id -i .ssh/my-key.pub docker-primary@ip`

Once it's copied - in /etc/ssh/sshd_config set the following lines

`PubkeyAuthentication yes`

`PasswordAuthentication no`

to log in just use 

`ssh -i ~/.ssh/my-key docker-primary@ip`

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
- [ ] Aggregate queries on MongoDB for faster data processing
- [ ] Mongodb indexing and sharding
- [ ] SSL termination 

# References:
[rootless-docker-security](https://docs.docker.com/engine/security/rootless/)
[Apollo Docs](https://www.apollographql.com/docs/apollo-server/getting-started)
