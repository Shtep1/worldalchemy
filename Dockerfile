FROM node:22-alpine
WORKDIR /app
COPY server.js index.html package.json ./
ENV PORT=8080 DATA_FILE=/data/data.json
VOLUME /data
EXPOSE 8080
CMD ["node", "server.js"]
