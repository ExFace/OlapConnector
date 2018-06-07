# Querying OLAP Cubes in SSAS via SQL

You can embed MDX queries in SQL and let SqlServer perform them. This does not require any specific server setup, but might have performance issues.

**IMPORTANT**: This approach only works if the connection to SqlServer is using Windows Authentication and the corresponding windows user (the one, the PHP server runs under) has access to the required cubes.


## Create a connection in the meta model

Go to Administration > Metamodel > Connections and press `New`. Use the `MsSqlMdxConnector` and add the following data connection properties **in addition** to the ones required for regular SQL access:

- `olap_catalog`

## Create a data source

Proceed to Administration > Metamodel > Data Sources and add a new data source with the `SsasMdxBuilder` as query builder and the connection just created.