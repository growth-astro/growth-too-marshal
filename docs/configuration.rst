Configuration
=============

.. code-block:: python
   :caption: application.cfg

   # Sample application.cfg file

   # Server name for the Marshal
   # Note: Without this, server name will default to localhost.
   SERVER_NAME = 'skipper.caltech.edu:8081'

   # Default email that receives notifications
   # Note: Without this, email alerts will be broken.
   EMAIL_TOO = "XXXXXXXXXXXXXXX@gmail.com"
   # GMail based username and password
   MAIL_USERNAME = "XXXXXXXXXXXXXX@gmail.com"
   MAIL_PASSWORD = "XXXXXXXXXXXXXXXX"

   # Twilio account parameters
   # Note: Without this, phone and text alerts will be broken.
   TWILIO_ACCOUNT_SID = 'XXXXXXXXXXXXXXXXXXXXXXXXX'
   TWILIO_AUTH_TOKEN = 'XXXXXXXXXXXXXXXXXXXXXXXXX'
   TWILIO_FROM = 'XXXXXXXXXXXXXXXXXXXXX'


.. code-block:: text
   :caption: .netrc

   # Sample .netrc file
   # Without this, access to TAP interface will be broken (i.e.
   # reference image coverage, observations, etc.).
   machine irsa.ipac.caltech.edu login XXXXXXXXX password XXXXXXXXXXXXX
