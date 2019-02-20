.. _internal_getting_started:

Internal Dataverk Project - Getting Started
=============================================

dataverk-cli supports an internal project use case. Internal projects are project that requires integration with
the organisations internal tools. When calling dataverk-cli init with the -i or --internal flag set, dataverk will fetch
settings files and configurations from specified settings and template repositories.

Common integrations include:
 * Organisation CI platform
 * Organisation container platform
 * Organisation secrets store



Dataverk consists of two main components
-----------------------------------------
* **dataverk** - python package with classes and functions for programming the ETL job
* **dataverk-cli** - commandline tool to setup the initial project structure, convert etl notebooks to scripts, publish the etl result and schedule the etl job


Installation
--------------
.. code-block:: bash

    pip install dataverk



Creating your first Dataverk project
------------------------------------

Dataverk expects one git repository per datapackage.

Start with creating a new Github repository and clone it to your local machine.

.. code-block:: bash

    git clone https://github.com/my-user/my-datapackage.git
    cd my-datapackage

For dataverk to work it needs a Github token to commit deploy keys, and NAV credentials to access Vault secrets store and Jenkins job scheduler.
The .env file also contains urls to appropriate template and settings files.
We store it in a local .env file, each individual datapackage needs its own .env file.

.. code-block:: bash

    dataverk-cli create-env-file


Dataverk initialize a new datapackage project with template files and settings.

.. code-block:: bash

    dataverk-cli init -i (optionally add --package-name to avoid being prompted)


Edit the etl.ipynb in your favorite notebook editor. For sample etl scripts, see https://github.com/datasett


If you want to publish the datapackage once only (not on a schedule) you can do this with the dataverk-cli publish command.

.. code-block:: bash

    dataverk-cli publish


If you want to schedule your ETL script you need to first add, commit and push the local repository to Github so Jenkins knows where
to find the Jenkinsfile. Jenkins is listening to the master branch.

.. code-block:: bash

    git add .
    git commit -m "your commit message"
    git push origin master


You can now define the schedule for you ETL job with the POSIX cron syntax, ref: https://support.acquia.com/hc/en-us/articles/360004224494-Cron-time-string-format

.. code-block:: bash

    dataverk-cli schedule (optionally add --update-schedule to avoid being promted)

When you now commit the new job schedule to Github, Jenkins will automatically run your ETL script on Nais as per the schedule.

.. code-block:: bash

    git add .
    git commit -m "your commit message"
    git push origin master

If you want to remove a scheduled job use the dataverk-cli delete.

.. code-block:: bash

    dataverk-cli delete (will delete jenkins-job and delete the files in the local repo)

