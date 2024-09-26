Debugging Flask and Docker instances
###########################################################

IDE debugging of functions
=============================================

* Set up python virtual environment for the project
* Connect IDE to venv python interpreter

Local debugging of flask app
=============================================

* Run flask at command prompt

```zsh
export FLASK_ENV=development
export FLASK_APP=flask_app.bison.routes
flask run
```
* The development port will be 5000.  Connect to
  http://127.0.0.1:5000 in browser,

  * Broker
    http://127.0.0.1:5000/api/v1/describe

* Flask will auto-update on file save.
* Refresh browser after changes

Local debugging of python scripts
--------------------------------------

* Step 1, annotate RIIS with GBIF accepted taxa:

  * directly run script to annotate RIIS in EC2 docker container.  The script will
    execute from the /home/bison directory, which contains the venv (python virtual
    environment) and bison directories::

     sudo docker exec bison-bison-1 venv/bin/python bison/tools/annotate_riis.py

  * If script fails, try interacting with the container directly.::

    sudo docker exec -it bison-bison-1 /bin/sh
    $ pwd
    /home/bison
    $ venv/bin/python


* Step 7.

Run Docker containers (development)
-------------------------------------------

Note that the development compose file, docker-compose.development.yml, is referenced
first on the command line.  It has elements that override those defined in the
general compose file, docker-compose.yml::

    sudo docker compose -f docker-compose.development.yml -f docker-compose.yml  up

Flask has hot-reload enabled.

Rebuild/restart
-------------------------------------------

To delete all containers, images, networks and volumes, stop any running
containers::

    sudo docker compose stop


And run this command (which ignores running container)::

    sudo docker system prune --all --volumes

Then rebuild/restart::

    sudo docker compose -f docker-compose.development.yml -f docker-compose.yml  up

Examine container
-------------------------------------------

To examine flask container at a shell prompt::

    sudo docker exec -it bison-nginx-1 /bin/sh

Or backend container::

    sudo docker exec -it bison-bison-1 /bin/sh
