Deploy BISON
##############################

Setup AWS resources
=======================================

`AWS Resource Setup <aws/aws_setup>`_.

from Github repo in /home/ubuntu/

Create SSL certificates
============================

`SSL certificate installation <ssl_certificates>`_.


Build/deploy BISON
================================

Environment status checks:
--------------------------

* Ensure the FQDN(s) in .env.*.conf and server/server_name in nginx.conf agree for each
  subdomain deployment.

  .env.conf::

    FQDN=bison.localhost

  nginx.conf::

    server {
      ...
      server_name  bison.localhost;


* Ensure no webserver (i.e. apache2) is running on the host machine
* Ensure the SSL certificates are present on the host machine, and visible to the
  containers.
* Ensure that the port in the deployment command in the Dockerfile (for running
  flask in the appropriate development or production flask container) matches the
  appropriate docker-compose file.  The development and production commands point
  to different flask applications via the variables FLASK_APP and FLASK_MANAGE, defined
  in the docker compose files.  The command also indicates which port the app runs on:
  5000 for FLASK_APP on production, DEBUG_PORT for FLASK_MANAGE on development.

  Dockerfile::

        # Production flask image from base
        ...
        CMD venv/bin/python -m gunicorn -w 4 --bind 0.0.0.0:5000 ${FLASK_APP}

  docker-compose.yml::

      services:
        bison:
          ...
          environment:
            - FLASK_APP=flask_app.bison.routes:app
          ...

  Dockerfile::

        # Development flask image from base
        FROM base as dev-flask
        ...
        CMD venv/bin/python -m debugpy --listen 0.0.0.0:${DEBUG_PORT} -m ${FLASK_MANAGE} run --host=0.0.0.0

  docker-compose.development.yml::

      bison:
        ...
        ports:
          - "5001:5001"
        environment:
          - FLASK_APP=flask_app.bison.routes:app
          - FLASK_MANAGE=flask_app.bison.manage
          - DEBUG_PORT=5001
        ...


* The proxy pass in nginx.conf points to the container
  name (http://container:port) using http (even for SSL); it points to port 5000
  for each container.  The Dockerfile command indicates which port the app runs on (5000
  for FLASK_APP on production, DEBUG_PORT for FLASK_MANAGE on development.
  (TODO: clarify this!
  https://maxtsh.medium.com/a-practical-guide-to-implementing-reverse-proxy-using-dockerized-nginx-with-multiple-apps-ad80f6dfce17):

nginx.conf::

    # Bison
    server {
      listen 443 ssl;
      location / {
        ...
        # pass queries to the bison container
        proxy_pass http://bison:5000;
      ...

Host machine preparation
----------------------------
These tasks should be set up on the EC2 host, possibly in the userdata
script run on instantiation.

* Create a directory, /home/ubuntu/aws_data, on the host machine to share data with
  docker containers.

  * The aws_data directory will contain matrix data downloaded from S3
    for data analysis outputs used by the BISON APIs.

    * download input data into this directory prior to volume creation in deployment
      (docker compose).  If the data is not present, the application will download the
      data to the WORKING_DIR directory set in .env.conf

      * TODO: set up an automated task to download this on creation in S3

    * docker-compose.yml bind-mounts this host directory to the /volumes/aws_data
      directory as Read-Only on the backend (bison) container.
    * AWS_INPUT_DATA in the .env.conf file points to this volume
    * AWS_INPUT_PATH in python code references the AWS_INPUT_DATA environment variable
      in the flask_app/bison/base.py service


Standard manipulation
=================================

Edit the docker environment files
-------------------------------------------

* Add the deployment FQDN to the file .env.conf and nginx.conf
* Change the FQDN value to the fully qualified domain name of the server.

  * If this is a local testing deployment, it will be "localhost"
  * For a development or production server it will be the FQDN with correct subdomain,
    i.e FQDN=bison.spcoco.org or bison-dev.spcoco.org in .env.conf

Run the containers (production)
-------------------------------------------

Start the containers with the Docker composition file::

    sudo docker compose -f docker-compose.yml up -d

BISON web services are now available at https://bison.spcoco.org/

Make sure the host machine is not running a webserver (apache2) which will bind
the http/https ports and not allow the docker containers to use them.


Run the containers (development)
-------------------------------------------

Note that the development compose file, docker-compose.development.yml, is referenced
first on the command line.  It has elements that override those defined in the
general compose file, docker-compose.yml::

    sudo docker compose -f docker-compose.development.yml -f docker-compose.yml  up

BISON web services are now available at https://localhost/ or
https://bison-dev.spcoco.org/

Flask has hot-reload enabled, so changes in code take effect immediately.


Rebuild/restart
-------------------------------------------

To delete all containers, images, networks and volumes, stop any running
containers::

    sudo docker compose stop


And run this command (which ignores running container)::

    sudo docker system prune --all --volumes

Then rebuild/restart::

    sudo docker compose up -d
    # or
    sudo docker compose -f docker-compose.development.yml -f docker-compose.yml  up

Examine container
-------------------------------------------

To examine containers at a shell prompt::

    sudo docker exec -it bison-nginx-1 /bin/sh

Error port in use:
"Error starting userland proxy: listen tcp4 0.0.0.0:443: bind: address already in use"

See what else is using the port.  In my case apache was started on reboot.  Bring down
all docker containers, shut down httpd, bring up docker.

::
    lsof -i -P -n | grep 443
    sudo docker compose down
    sudo systemctl stop httpd
    sudo docker compose  up -d

Run Docker on OSX
=================================

Need to bind server to 0.0.0.0 instead of 127.0.0.1

Test by getting internal IP, using ifconfig, then command to see if connects successfully::

    nc -v x.x.x.x 443

Then can use same IP in browser, i.e. https://x.x.x.x/api/v1/name/
This only exposes the bison, not the analyst services.
