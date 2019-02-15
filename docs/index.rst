.. Dataverk documentation master file, created by
   sphinx-quickstart on Fri Feb  8 13:12:48 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to Dataverk's documentation!
====================================

Dataverk is a Python package and commandline tool to create and program a Python based ETL(Extract Transform Load) job.

Dataverk consists of two main components
-----------------------------------------
* **dataverk** - python package with classes and functions for programming the ETL job
* **dataverk-cli** - commandline tool to setup the initial project structure, convert etl notebooks to scripts, publish the etl result and schedule the etl job


Installation
--------------
::

   pip install dataverk


Creating your first Dataverk project
--------------------------------------
**NB:** Before running the dataverk-cli init commmand it is important that a git repository has already been created in the target directory.

::

   mkdir new_dataverk_project
   cd new_dataverk_project
   git init
   dataverk-cli init


.. toctree::
   :maxdepth: 2
   :caption: Contents:

   api-docs/datapackage
   api-docs/api
   internal-project/getting-started




Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
