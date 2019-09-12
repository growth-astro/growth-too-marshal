Deployment
==========

For production, the GROWTH ToO Marshal is deployed using `Docker`_.

Getting the Docker image
------------------------

Pull latest Docker image Docker Hub::

    docker pull growthastro/growth-too-marshal

Or build the Docker image locally::

    docker-compose build

In case you need to manually push a locally built image to Docker Hub::

    docker build -t growthastro/growth-too-marshal .
    docker push growthastro/growth-too-marshal

Running the Marshal
-------------------

Initialize the database and populate it with some sample alerts::

    docker-compose run celery db create --sample

Start the ToO Marshal (navigate to ``http://localhost:8081/`` in your browser)::

    docker-compose up -d

Stop the ToO Marshal::

    docker-compose down

.. _`Docker`: https://www.docker.com

Troubleshooting
---------------

Run an interactive PostgreSQL shell::

    docker-compose run --rm postgres psql -h postgres -U postgres

Run an interactive Python shell::

    docker-compose run --rm redis redis-cli -h redis

Run an interactive Flask (Python) shell::

    docker-compose run --rm --entrypoint growth-too flask shell

Run an interactive Celery (Python) shell::

    docker-compose run --rm celery celery shell
