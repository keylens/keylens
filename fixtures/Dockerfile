FROM node:22-alpine

WORKDIR /app

# Copy the manifest first so `npm install` is cached across producer.mjs edits.
COPY package.json ./
RUN npm install --omit=dev

COPY producer.mjs ./

CMD ["node", "producer.mjs"]
