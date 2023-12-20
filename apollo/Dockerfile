# Use an official Node base image
FROM node:20.7.0

# Set the working directory in the container
WORKDIR /usr/app

# Copy the package.json file and other necessary files
COPY package*.json tsconfig.json .env ./
COPY index.ts ./src/index.ts

RUN npm init --yes && npm pkg set type="module"

# Install dependencies
RUN npm install typescript @apollo/server graphql graphql-tag mongodb dotenv joi ts-node-dev nodemon

RUN npm install -D @types/graphql @types/node

# Expose the port the app runs on
EXPOSE 4000

# Define the command to run the app using CMD which defines the runtime
CMD ["npm", "start"]
