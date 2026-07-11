# WIS2-GlobalBroker-Redundancy

The code in this repository is to provide a reference implementation of a Global Broker as defined in the technical specification of WIS2. 
See [https://community.wmo.int/en/WIS2_Technical_Specification_Guidance](https://wmo-im.github.io/wis2-guide/guide/wis2-guide-APPROVED.html)

This is the second version of a reference implementation of the AntiLoop feature that is part of a Global Broker.
It has been built reusing the vast majority of the first version of the tool available here: https://github.com/golfvert/WIS2-GlobalBroker-NodeRed

It is considered that providing a cluster of an MQTT broker is a key component of a Global Broker. 

However, unlike the initial version, this one adds redundancy where needed:
- It uses an *available* redis cluster (See https://redis.io/docs/management/scaling/ on how to create such a cluster)
- Each instance of the antiloop docker container can be run multiple times (this would be equivalent to, in a docker swarm environment, redundancy >= 2 ) toward the same WIS2 Node. Each instance will subscribe to the remote broker and only the *primary* will process the messages. The containers are running in an active/active more for the subscription point of view but in an active/passive point of view for the processing. 

Keeping two active subscription guarantee that no message will be lost in case of failure of one of the instance of the container. Messages on the *secondary* container(s) are queued to allow promotion to *primary* without message loss.
The election procedure uses the redis cluster.

The repo has been cloned from the official NodeRed repository.

## What is here ?

1. This is the source code of the container `golfvert/wis2gb` and the required files to run the deduplication software in front of a Global Broker
2. Configuration files to run the container available at `golfvert/wis2gb`

## What does it do ?

1. Listen to subscribed topics from WIS2Node and other Global Brokers (one subscription per container)
2. Every 2s each container publishes on the *WIS2 Node centre_id* used as redis key, its UUID and the timestamp.
3. Every 10s each container verifies if its unique identifier is the lowest of all containers for this key. If yes, it stays *primary* or becomes the *primary* and messages stored in the holding queue are released. On *secondary* the holding queue is flushed.
4. Optionally verify/discard/ignore the message for its validity compared to approved message format
5. Look at the `id` in the message. 
6. Through a redis request check if that `id` has already been seen in the last 2 hours. If yes, simply discard the message
7. If not, publish the message to the attached Global Broker
8. It also provides prometheus metrics available at http://@IP:1880/metrics

## How to use it ?

Download 
- docker-compose.yaml

and edit:

Copy & Paste the wis2node_1 to get one container per WIS2Node or other Global Brokers / Global Cache to subscribe to. Do NOT subscribe to the local Global Broker.
- Change the name of the container (make sure it is unique!)
- Change all MQTT_SUB_* to connect to the remote broker and to the topic from that source. In the example below `origin/a/wis2/fra/#` will subscribe to all topic from France according to WIS2 agreed topic hierarchy. It can be a list of topics separated by ",". Such as `origin/a/wis2/#,cache/a/wis2/#`. This is, typically, useful when subscribing to another Global Broker.
- Change ports 1st 1880:1880, 2nd 1881:1880,... or run behind a traefik proxy (this is the preferred method).

```
  wis2node_1:
    container_name: wis2node_1
    image: golfvert/wis2gb
    environment:
      - TZ=Europe/Paris
      - MQTT_SUB_BROKER=Broker_URL    # WIS2Node URL broker such as mqtts://broker.example.com:8883 or wss://broker.example.com:443
      - MQTT_SUB_USERNAME=
      - MQTT_SUB_PASSWORD=
      - MQTT_SUB_TOPIC=Topic_to_sub   # e.g. origin/a/wis2/fr-meteofrance/#. 
      - MQTT_SUB_VERIFYCERT= true     # if using SSL should the certificate by checked (prevent slef-signed certificates to work. Or not)
      - MQTT_PUB_BROKER=GlobalBroker_URL   # Global Broker URL such as mqtts://globalbroker.site.com:8883 or wss://globalbroker.site.com:443
      - MQTT_PUB_USERNAME=
      - MQTT_PUB_PASSWORD=
      - MSG_CHECK_OPTION=verify      # Should messages be "verify" (just add _comment in the notification message), "discard" (bin the message if not correct), "ignore" (don't check the messages). This ensures compliance with WIS2 Notification Message specifications.
      - METADATA_CHECK_OPTION=verify    # Should topic of publication be verified against the metadata published by centreid. The list is obtained by querying the Global Discovery Catalog.
      Query is made every 15 minutes.
      - TOPIC_CHECK_OPTION=verify    # Should topic of publication be verified against schemas.wmo.int. The list is obtained by querying https://github.com/golfvert/wis2-topic-hierarchy
      This repository contains a list of hashed topic hierarche. The list is updated on average once a day.
      - GDC_URL=                     # How to query the GDC ? centre-id is added at the end of the URL.
      - CENTRE_ID=                   # Name_of_Center used as label and as a key when 2 (or more) containers are running 
      - REDIS_URL=[{"host":@IP1,"port":port1},{"host":@IP2,"port":port2},......] # A JSON Array with all host:port instances of the redis cluster
    ports:
      - "1880:1880"
    networks:
      - wis2relay
 ```
 
There must be a redis *cluster* running and reachable by the wis2gb container

CENTRE_ID is used as a variable as label in Prometheus. It is also used as a key when multiple containers are configured for one WIS2 Node

When done, save the docker-compose.yaml and start it with `docker compose up -d`

It will  subscribe to the "remote" destination (WIS2node(s), Global Caches, other Global Brokers) and will publish to the local Global Broker.

## Check Global Broker Anti-Loop Node-Red container behaviour
Node-red interface is available at http://@IP:1880/admin/ (unless behing a proxy or other changes - eg. port). Accessing this interface will show the flows in action. Adding debug nodes may help in understaning the function of the anti-loop software. This interface is NOT password protected. Typically unprotected access should be prevented through port filtering and proxying the access via traefik.

## How to modify it ?

This is a fork from the official Node-Red repo. Follow official documentation to tweak it to your needs.
