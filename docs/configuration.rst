Configuration
=============

.. code-block:: python
   :caption: application.cfg

   # Sample application.cfg file
   import os

   # Database paths
   CELERY_BROKER_URL = 'redis://redis'
   SQLALCHEMY_DATABASE_URI = 'postgresql://postgres:mysecretpassword@postgres/postgres'

   # Server name for the Marshal
   # Note: Marshal will still have functionality if not specified
   SERVER_NAME = 'skipper.caltech.edu:8081'

   # Default email that receives notifications
   # Note: Marshal will still have functionality if not specified
   EMAIL_TOO="XXXXXXXXXXXXXXX@gmail.com"
   # GMail based username and password
   MAIL_USERNAME="XXXXXXXXXXXXXX@gmail.com"
   MAIL_PASSWORD="XXXXXXXXXXXXXXXX"

   # Twilio account parameters
   # Note: Marshal will still have functionality if not specified
   TWILIO_ACCOUNT_SID = 'XXXXXXXXXXXXXXXXXXXXXXXXX'
   TWILIO_AUTH_TOKEN = 'XXXXXXXXXXXXXXXXXXXXXXXXX'
   TWILIO_FROM = 'XXXXXXXXXXXXXXXXXXXXX'


.. code-block:: text
   :caption: .netrc

   # Sample .netrc file
   # Access to TAP interface
   machine XXXXXXXXXXXXX login XXXXXXXXX password XXXXXXXXXXXXX
