COSMOS (COS Monitoring System)
==============================

Welcome! This is the documentation for the COS Monitoring System (COSMOS). This document aims to give a high level overview of COSMOS and you can get your copy here:
`COSMOS <https://github.com/justincely/cos_monitoring>`_

What is COSMOS?
---------------

COSMOS is a backend database that is structured specifically for the Cosmic Origins Spectrogragh (COS) team at Space Telescope Science
Institute (STScI). COSMOS uses a `MySQL <https://dev.mysql.com/doc/>`_ database server with
`SQL Alchemy <http://docs.sqlalchemy.org/en/rel_1_0/>`_ object-relational mapper (ORM) for python. This allows for interfacing with monitors
that are important for extending the lifetime of COS. The database is broken up into many different relational tables which include header keyword and monitoring data.
These tables are then queried by the monitors to generate figures and other deliverables like reference files with delivery forms in a format for direct delivery to CDBS.

How do I access COSMOS?
-----------------------

First, you will need to ask ITSD for a username and password to access the COSMOS database. Once you have these credentials
to access COSMOS you will need to SSH into one of the INS computing clusters (plhstins1, plhstins2, plhstins3). After you are on the cluster you will need to create a file titled
'configure.yaml' in your home directory that contains the information that was given to you by ITSD. An example of how your configure.yaml file should look is located below ::

  #an example of a configure.yaml file

  user: 'user_name'
  password: 'string_of_random_characters'
  port: port_number
  host: 'greendev.stsci.edu'
  database: 'cos_cci'
  connection_string: 'mysql+pymysql://user:password@greendev.stsci.edu:port/cos_cci'
  data_location: '/smov/cos/Data/'
  num_cpu: 16

.. admonition:: NOTE
   :class: note

   | In the ``conection_string`` variable above where ``user``, ``password``, and ``port`` are located, ENTER YOUR ACTUAL USERNAME, PASSWORD, AND PORT into the string.
   |
   | If you are wondering about your password and port number, the password will be a sting of random characters i.e. have single or double quotes around it and the port
     will be a much shorter integer i.e. no single or double quotes around it.


After you have created your configure.yaml file, now it is time to access the database. After your configure.yaml file is finished now you will need to enter this following
command ::

  $ mysql -hgreendev -u username -Pportnumber -ppassword

where ``h, u, P, p`` represent host, user, Port, and password respectively.

You should now be in a mysql session which should change the terminal prompt to ``mysql>``.

Navigating COSMOS
-----------------

To see the databases that are hosted you can enter the next command ::

  mysql> SHOW DATABASES;

  +--------------------+
  | Database           |
  +--------------------+
  | information_schema |
  | cos_cci            |
  +--------------------+

and the database we want to use is ``cos_cci``. To access the the database enter ::

  mysql> USE cos_cci;

now you are in the cos_cci database used by COSMOS. The database will contain tables, a table is a collection of related data in an organized fashion in the database.
This can be done by entering the following command ::

  mysql> SHOW TABLES;

  +-------------------+
  | Tables_in_cos_cci |
  +-------------------+
  | darks             |
  | data              |
  | files             |
  | flagged           |
  | gain              |
  | headers           |
  | lampflash         |
  | phd               |
  | spt               |
  | stims             |
  +-------------------+

Another import SQL intrinsic you may want to use is the ``DESCRIBE`` which will return a high level overview of how the table is constructed. As an example we will look at
the stims table using ``DESCRIBE`` which returns ::

  mysql> DESCRIBE stims;

  +----------+------------+------+-----+---------+----------------+
  | Field    | Type       | Null | Key | Default | Extra          |
  +----------+------------+------+-----+---------+----------------+
  | id       | int(11)    | NO   | PRI | NULL    | auto_increment |
  | time     | float      | YES  |     | NULL    |                |
  | rootname | varchar(9) | YES  | MUL | NULL    |                |
  | abs_time | float      | YES  |     | NULL    |                |
  | stim1_x  | float      | YES  |     | NULL    |                |
  | stim1_y  | float      | YES  |     | NULL    |                |
  | stim2_x  | float      | YES  |     | NULL    |                |
  | stim2_y  | float      | YES  |     | NULL    |                |
  | counts   | float      | YES  |     | NULL    |                |
  | segment  | varchar(4) | YES  |     | NULL    |                |
  | file_id  | int(11)    | YES  | MUL | NULL    |                |
  +----------+------------+------+-----+---------+----------------+

the ``Field`` which you can think of as a the column header, ``Type`` which is the datatype of the column, ``Null`` which shows whether NULLS are present, ``Key``
which shows the type of key, ``Default`` which is the default option if the value wasn't located, and ``Extra`` which tell you if the ``Field`` has any other functionality to it.

.. admonition:: NOTE
   :class: note

   | You may have noticed by now that it is convention to use capital letters for SQL intrinsic commands and lowercase for tables, databases, etc.
   | **TIP:** You can terminate an SQL query with a semicolon and for longer queries you can use enter to break the lines up before terminating with a semicolon.

Higher Level SQL with COSMOS
----------------------------
Now that you have some basic exposure on how to navigate COSMOS it's time to show some of the power of COSMOS and some queries that may interest you.

The simplest example of an SQL query is as follows. ::

  mysql> SELECT field(s) FROM table;

.. admonition:: NOTE
   :class: note

   | The unix wildcard * is a valid for the field(s) argument.
   | **Example:** If you wanted all of the fields and rows from the stims table you would enter ``mysql> SELECT * from stims;``


COSMOS Monitors
---------------
.. toctree::
   :maxdepth: 2

   database_mods
   osm_mods
   stim_mods
   utils_mods
   dark_mods
   retrieval


Need Help?
==========
Contact

Justin Ely: ely@stsci.edu

Jo Taylor: jotaylor@stsci.edu

Mees Fix: mfix@stsci.edu


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
