-------------------------------------------------
-- NAME:	 IMPL-IS-33-cluster-information.txt
-------------------------------------------------
-- DESCRIPTION:
--	Provides understanding around usage of system function clustering information for 
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
-- 18JAN22	WNA		created/updated for repository
-------------------------------------------------
	
---------------------------
-- syntax
---------------------------
--SYSTEM$CLUSTERING_INFORMATION( '<table_name>' , '( <col1> [ , <col2> ... ] )' )

---------------------------
-- parameters
---------------------------
--table_name
--Table for which you want to return clustering information.

--col1 [ , col2 ... ]
--Column(s) in the table for which clustering information is returned:

--For a table with no clustering key, this argument is required. If this argument is omitted, an error is returned.

--For a table with a clustering key, this argument is optional; if the argument is omitted, Snowflake uses the defined clustering key to return clustering information.

--You can use this argument to return clustering information for any columns in the table, regardless of whether a clustering key is defined for the table.

--In other words, you can use this to help you decide what clustering to use in the future.
---------------------------
-- usage
---------------------------
--All arguments are strings (i.e. they must be enclosed in single quotes).

---------------------------
-- example
---------------------------

--select system$clustering_information('test2', '(col1, col3)');

--+--------------------------------------------------------------+
--| SYSTEM$CLUSTERING_INFORMATION('TEST2', '(COL1, COL3)')       |
--|--------------------------------------------------------------|
--| {                                                            |
--|   "cluster_by_keys" : "(COL1, COL3)",                        |
--|   "total_partition_count" : 1156,                            |
--|   "total_constant_partition_count" : 0,                      |
--|   "average_overlaps" : 117.5484,                             |
--|   "average_depth" : 64.0701,                                 |
--|   "partition_depth_histogram" : {                            |
--|     "00000" : 0,                                             |
--|     "00001" : 0,                                             |
--|     "00002" : 3,                                             |
--|     "00003" : 3,                                             |
--|     "00032" : 98,                                            |
--|     "00064" : 269,                                           |
--|     "00128" : 698                                            |
--|   }                                                          |
--| }                                                            |
--+--------------------------------------------------------------+
--This example indicates that the test2 table is not well-clustered for the following reasons:

--Zero (0) constant micro-partitions out of 1156 total micro-partitions.

--High average of overlapping micro-partitions.

--High average of overlap depth across micro-partitions.

--Most of the micro-partitions are grouped at the lower-end of the histogram, with the majority of micro-partitions having an overlap depth between 64 and 128.