FROM node:lts-alpine AS builder

WORKDIR /app

COPY . .

RUN npm install

RUN npm run start

EXPOSE 8010