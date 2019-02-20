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
**NB:** Before running the dataverk-cli init commmand it is important that a git repository has already been created in the target directory.

.. code-block:: bash

    dataverk-cli create-env-file
    dataverk-cli init -i (optionally add --package-name to avoid being prompted)
    git add -> commit -> push
    (optional)dataverk-cli publish (used when wanting to publish the data but not schedule a job)

    dataverk-cli schedule (optionally add --update-schedule to avoid being promted)
    git add -> commit -> push
    dataverk-cli delete (will delete jenkins-job and delete the files in the local repo)