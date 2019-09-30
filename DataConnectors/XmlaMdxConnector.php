<?php
namespace exface\OlapDataConnector\DataConnectors;

use exface\Core\CommonLogic\AbstractDataConnectorWithoutTransactions;
use exface\Core\Interfaces\DataSources\DataQueryInterface;
use phpOLAPi\Xmla\Connection\Connection;
use phpOLAPi\Xmla\Connection\Adaptator\SoapAdaptator;
use exface\OlapDataConnector\MdxDataQuery;
use exface\Core\Exceptions\DataSources\DataConnectionQueryTypeError;
use exface\Core\Exceptions\DataSources\DataQueryFailedError;
use phpOLAPi\Renderer\AssocArrayRenderer;

class XmlaMdxConnector extends AbstractDataConnectorWithoutTransactions
{
    private $connection = null;
    
    private $server = null;
    
    private $user = null;
    
    private $password = null;
    
    private $catalogName = null;
    
    protected function performConnect()
    {
        $this->connection = new Connection(
            new SoapAdaptator($this->getServer(), $this->getUser(), $this->getPassword()),
                [
                    'DataSourceInfo' => null,
                    'CatalogName' => $this->getCatalogName()
                ]
            );
    }
    
    protected function performDisconnect()
    {
        return $this;
    }

    protected function performQuery(DataQueryInterface $query)
    {
        if (! ($query instanceof MdxDataQuery)) {
            throw new DataConnectionQueryTypeError($this, 'Cannot handle "' . get_class($query) . '": expectin an MdxDataQuery!');
        }
        
        try {
            $resultSet = $this->getConnection()->statement($query->getMdx());
            $query->setResultArray((new AssocArrayRenderer($resultSet))->generate());
        } catch (\Throwable $e) {
            throw new DataQueryFailedError($query, 'Error in MDX query: ' . $e->getMessage(), null, $e);
        }
        
        return $query;
    }

    protected function getConnection() : Connection
    {
        return $this->connection;
    }

    /**
     * 
     * @return string
     */
    public function getServer() : string
    {
        return $this->server;
    }

    /**
     * The server URI for the XMLA endpoint
     * 
     * @uxon-property server
     * @uxon-type uri
     * 
     * @param string $uri
     * @return XmlaMdxConnector
     */
    public function setServer(string $uri) : XmlaMdxConnector
    {
        $this->server = $uri;
        return $this;
    }

    /**
     * 
     * @return string|NULL
     */
    public function getUser() : ?string
    {
        return $this->user;
    }

    /**
     * The user name for basic HTTP authentication
     * 
     * @uxon-property user
     * @uxon-type string
     * 
     * @param string $username
     * @return XmlaMdxConnector
     */
    public function setUser(string $username) : XmlaMdxConnector
    {
        $this->user = $username;
        return $this;
    }

    /**
     * 
     * @return string|NULL
     */
    public function getPassword() : ?string
    {
        return $this->password;
    }

    /**
     * The password for basic HTTP authentication
     * 
     * @uxon-property password
     * @uxon-type password
     * 
     * @param string $password
     * @return XmlaMdxConnector
     */
    public function setPassword(string $password) : XmlaMdxConnector
    {
        $this->password = $password;
        return $this;
    }


    public function getCatalogName() : string
    {
        return $this->catalogName;
    }

    /**
     * The name of the catalog to be used.
     * 
     * @uxon-property catalog_name
     * @uxon-type string
     * @uxon-required true
     * 
     * @param string $catalogName
     * @return XmlaMdxConnector
     */
    public function setCatalogName(string $catalogName) : XmlaMdxConnector
    {
        $this->catalogName = $catalogName;
        return $this;
    }

}