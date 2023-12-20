# Use an official Node base image
FROM node:20.7.0

# Set the working directory in the container
WORKDIR /usr/app

# Copy the package.json file and other necessary files
COPY package*.json tsconfig.json .env .eslintrc.cjs ./
COPY src ./src

RUN npm install
# Expose the port the app runs on
EXPOSE 4000

# Define the command to run the app using CMD which defines the runtime
CMD ["npm", "start"]
