Debugging Flask and Docker instances
###########################################################

IDE debugging of functions
=============================================

* Set up python virtual environment for the project
* Connect IDE to venv python interpreter

Local debugging
=============================================

Of flask app
--------------------------------------

* Run flask at command prompt

    ```zsh
    export FLASK_ENV=development
    export FLASK_APP=flask_app.bison.routes
    flask run
    ```

* The development port will be 5000.  Connect to
  http://127.0.0.1:5000 in browser,

  * Bison
    http://127.0.0.1:5000/api/v1/describe

* Flask will auto-update on file save.
* Refresh browser after changes

Of python scripts
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

Docker debugging
=============================================

Run Docker containers (development)
-------------------------------------------

Note that the development compose file, compose.development.yml, is referenced
first on the command line.  It has elements that override those defined in the
general compose file, compose.yml::

    sudo docker compose -f compose.development.yml -f compose.yml  up

Flask has hot-reload enabled.

Rebuild/restart
-------------------------------------------

To delete all containers, images, networks and volumes, stop any running
containers::

    sudo docker compose stop


And run this command (which ignores running container)::

    sudo docker system prune --all --volumes

Then rebuild/restart::

    sudo docker compose -f compose.development.yml -f compose.yml  up

Examine container
-------------------------------------------

To examine flask container at a shell prompt, or print logs::

    sudo docker exec -it bison-nginx-1 /bin/sh
    sudo docker logs bison-nginx-1 --tail 100

Or examine backend container::

    sudo docker exec -it bison-bison-1 /bin/sh

Troubleshooting
=================================

General debug messages for the flask container
----------------------------------------------

* Print logs::

  sudo docker logs bison-nginx-1 --tail 100

Problem: Failed programming external connectivity
--------------------------------------------------------

[+] Running 6/5
 ✔ Network bison_default        Created                                                                                                                                                          0.1s
 ✔ Network bison_nginx          Created                                                                                                                                                          0.1s
 ✔ Container bison-front-end-1  Created                                                                                                                                                          0.2s
 ✔ Container bison-bison-1     Created                                                                                                                                                          0.2s 0.2s
 ✔ Container bison-nginx-1      Created                                                                                                                                                          0.1s
Attaching to bison-1, front-end-1, nginx-1
Error response from daemon: driver failed programming external connectivity on endpoint
bison-nginx-1 (1feeaa264a757ddf815a34db5dd541f48d3f57aa21ef104e3d5823efbb35f9ab):
Error starting userland proxy: listen tcp4 0.0.0.0:80: bind: address already in use

Solution
...............

Stop apache2 on the host machine
