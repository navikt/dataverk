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
::

    pip install dataverk



Creating your first Dataverk project
------------------------------------
**NB:** Before running the dataverk-cli init commmand it is important that a git repository has already been created in the target directory.

::

    dataverk-cli create-env-file
    dataverk-cli init -i (eventuelt med --package-name for å unngå prompt)
    git add -> commit -> push
    (dataverk-cli publish) hvis man vil publisere uten å schedulere

    dataverk-cli schedule (eventuelt med --update-schedule for å unngå prompt) (edited)
    git add -> commit -> push
    dataverk-cli delete (fjerne jenkinsjobb og slette filer i local repo)