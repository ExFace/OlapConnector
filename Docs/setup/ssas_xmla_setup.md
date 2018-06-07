# Setting up an XMLA webservice on Microsoft SSAS

By default, Analysis Services are not accessible via webservice, so we need to set up the built-in IIS Webserver to pass requests from a certain URL to the Analysis Services.

## Configuring the IIS webserver

Follow the detailed tutorial on how to make IIS work with SSAS in the [Microsoft docs](https://docs.microsoft.com/de-de/sql/analysis-services/instances/configure-http-access-to-analysis-services-on-iis-8-0?view=sql-analysis-services-2017).

## Authentication

TODO

## Test the connection

You can use [postman](https://www.getpostman.com/) to test your connection: 

1) Create a new POST request to the URL you created in the previous step
2) Go to the tab and select `raw`
3) Paste the following code in the body and replace YOURCATALOG and YOURCUBE with the name of your catalog and cube respectively.

```xml
<Envelope xmlns="http://schemas.xmlsoap.org/soap/envelope/">
	<Body>
		<Execute xmlns="urn:schemas-microsoft-com:xml-analysis">
			<Command>
				<Statement>
					SELECT [Measures].MEMBERS ON COLUMNS FROM [YOURCUBE]
				</Statement>
			</Command>

			<Properties>
				<PropertyList>
					<Catalog>YOURCATALOG</Catalog>
					<!-- 
					<Format>Tabular</Format> 
					<Content>Data</Content> 
					-->
				</PropertyList>
			</Properties>

		</Execute>
	</Body>
</Envelope>
```

When you press `send`, you should receive an XML response from the server.

## Create a connection in the meta model

Go to Administration > Metamodel > Connections and press `New`. Use the `XmlaMdxConnector` and add the following data connection properties:

- `server` - the URL from above comes here (including "http://")
- `catalog_name` 
- `user` - only if you use basic authentication
- `passowrd` - only if you use basic authentication

## Create a data source

Proceed to Administration > Metamodel > Data Sources and add a new data source with the `SsasMdxBuilder` as query builder and the connection just created.