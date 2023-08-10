-------------------------------------------------
-- NAME:	 IMPL-AU-32-cluster-depth.txt
-------------------------------------------------
-- DESCRIPTION:
--	Provides understanding around usage of system function clustering depth for 
--	analyzing micro partitions
--
-- OUTPUT:
--	see notes below
--
-- NEXT STEPS:
--	determine new cluster key or need for re-clustering
--
-- OPTIONS:
--	see notes below
--
-- REVISION HISTORY
-- DATE		INIT	DESCRIPTION
----------  ----    -----------
-- 18JAN22	WNA		CREATED/UPDATED FOR REPOSITORY
-------------------------------------------------
	
---------------------------
-- syntax
---------------------------
--SYSTEM$CLUSTERING_DEPTH( '<table_name>' , '( <col1> [ , <col2> ... ] )' [ , '<predicate>' ] )

---------------------------
-- parameters
---------------------------
--table_name
--Table for which you want to calculate the clustering depth.

--col1 [ , col2 ... ]
--Column(s) in the table used to calculate the clustering depth:

--For a table with no clustering key, this argument is required. If this argument is omitted, an error is returned.

--For a table with a clustering key, this argument is optional; if the argument is omitted, Snowflake uses the defined clustering key to calculate the dept

--You can use this argument to calculate the depth for any columns in the table, regardless of the clustering key defined for the table.

--predicate
--Clause that filters the range of values in the columns on which to calculate the clustering depth. Note that predicate does not utilize a WHERE keyword at the beginning of the clause.

---------------------------
-- usage
---------------------------
--All arguments are strings (i.e. they must be enclosed in single quotes).

--If predicate contains a string, the string must be enclosed in single quotes, which then must be escaped using single quotes. For example:

--SYSTEM$CLUSTERING_DEPTH( ... , 'col1 = 100 and col2 = ''A''' )

---------------------------
-- example
---------------------------
--Calculate the clustering depth for a table using the clustering key defined for the table:

--select system$clustering_depth('TPCH_ORDERS');

--+----------------------------------------+
--| SYSTEM$CLUSTERING_DEPTH('TPCH_ORDERS') |
--|----------------------------------------+
--| 2.4865                                 |
--+----------------------------------------+
