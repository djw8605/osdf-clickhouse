-- Database for the xrootd monitoring pipeline.
-- ON CLUSTER '{cluster}' runs the DDL on every node; '{cluster}' is substituted
-- from the per-host macros the operator installs (cluster name = "xrootd").
CREATE DATABASE IF NOT EXISTS xrootd ON CLUSTER '{cluster}';
