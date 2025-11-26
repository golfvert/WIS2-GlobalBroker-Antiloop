FROM alpine:3.22.0 AS base


RUN apk add --no-cache \
            nodejs \
            ca-certificates \
            npm && \
    mkdir -p /usr/src/node-red /data && \
    adduser -h /usr/src/node-red -D -H node-red -u 1000 && \
    chmod 0777 /usr/src/node-red /data && \
    chown -R node-red:node-red /data 

FROM nodered/node-red:4.1.0-minimal AS build

COPY package.json .

RUN npm install \
        --unsafe-perm --no-update-notifier \ 
        --no-audit --only=production

FROM base AS prod

COPY --from=build --chown=node-red:node-red /data/ /data/

WORKDIR /usr/src/node-red
COPY settings.js /data/settings.js
COPY flows.json  /data/flows.json

COPY --from=build --chown=node-red:node-red /usr/src/node-red/  /usr/src/node-red/
USER node-red

CMD ["npm", "start", "--cache", "/data/.npm", "--", "--userDir", "/data"]
