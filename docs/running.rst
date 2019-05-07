Running the ToO Marshal
=======================

Use the ``growth-too`` command line tool for starting and managing the ToO
Marshal. The ``growth-too`` tool has a number of subcommands. The table below
is a quick guide to the most useful commands for development and testing.

+-----------------------+-----------------------------------------------------------+
| Task                  | Command line                                              |
+=======================+===========================================================+
| **Web application**                                                               |
+-----------------------+-----------------------------------------------------------+
| Run web app           | ``growth-too run --with-threads``                         |
+-----------------------+-----------------------------------------------------------+
| Run web app           | ``FLASK_ENV=development growth-too run --with-threads``   |
| (debugger enabled)    |                                                           |
+-----------------------+-----------------------------------------------------------+
| **Database**                                                                      |
+-----------------------+-----------------------------------------------------------+
| Initialize database   | ``growth-too db create``                                  |
+-----------------------+-----------------------------------------------------------+
| Initialize database,  | ``growth-too db create --sample``                         |
| populate with example |                                                           |
| events                |                                                           |
+-----------------------+-----------------------------------------------------------+
| Wipe database         | ``growth-too db drop``                                    |
+-----------------------+-----------------------------------------------------------+
| Wipe database, then   | ``growth-too db recreate``                                |
| initialize again      |                                                           |
+-----------------------+-----------------------------------------------------------+
| **Background processing**                                                         |
+-----------------------+-----------------------------------------------------------+
| Run Celery worker     | ``growth-too celery worker --loglevel info``              |
+-----------------------+-----------------------------------------------------------+
| Run GCN listener      | ``growth-too gcn``                                        |
+-----------------------+-----------------------------------------------------------+
| Run periodic task     | ``growth-too celery beat``                                |
| scheduler             |                                                           |
+-----------------------+-----------------------------------------------------------+
| Run Flower console    | ``growth-too celery flower``                              |
+-----------------------+-----------------------------------------------------------+
| **Admin**                                                                         |
+-----------------------+-----------------------------------------------------------+
| Enter Python console  | ``growth-too shell``                                      |
+-----------------------+-----------------------------------------------------------+
| Add user/password     | ``growth-too passwd``                                     |
+-----------------------+-----------------------------------------------------------+
