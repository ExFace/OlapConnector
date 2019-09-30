<?php
namespace exface\OlapDataConnector\DataConnectors;

use exface\Core\DataConnectors\MsSqlConnector;
use exface\Core\Interfaces\DataSources\DataQueryInterface;
use exface\Core\CommonLogic\DataQueries\SqlDataQuery;
use exface\OlapDataConnector\MdxDataQuery;

/**
 * Data Connector to perform MDX queries on Microsoft Analytics Services via TSQL (EXPERIMENTAL!). 
 * 
 * This connector uses a linked server for the respective OLAP cube, which it creates
 * when connecting and drops when disconnecting:
 * 
 * ```
 * sp_addlinkedserver 
 *     @server='EXFOLAP', 
 *     @srvproduct='[#olap_srvproduct#]',
 *     @provider='[#olap_provider#]', 
 *     @datasrc='[#olap_datasrc#]', 
 *     @catalog='[#olap_catalog#]'
 * 
 * ```
 *
 * @author Andrej Kabachnik
 *        
 */
class MsSqlMdxConnector extends MsSqlConnector
{
    private $olapSrvproduct = '';
    
    private $olapDatasrc = '';
    
    private $olapCatalog = '';
    
    private $olapProvider = 'MSOLAP';

    /**
     *
     * {@inheritdoc}
     *
     * @see \exface\Core\CommonLogic\AbstractDataConnector::performConnect()
     */
    protected function performConnect()
    {
        parent::performConnect();
        $initLinkedServer = <<<SQL

EXEC sp_addlinkedserver 
    @server='EXFOLAP', 
    @srvproduct='{$this->getOlapSrvproduct()}',
    @provider='{$this->getOlapProvider()}', 
    @datasrc='{$this->getOlapDatasrc()}', 
    @catalog='{$this->getOlapCatalog()}'

SQL;
        $this->runSql($initLinkedServer);

    }
    
    /**
     * 
     * {@inheritDoc}
     * @see \exface\Core\DataConnectors\AbstractSqlConnector::performQuery()
     */
    protected function performQuery(DataQueryInterface $query)
    {
        if ($query instanceof MdxDataQuery) {
            $sqlQuery = new SqlDataQuery();
            $sqlQuery->setSql($this->buildSqlWrapper($query->getMdx()));
            return parent::performQuery($sqlQuery);
        }
            
        return parent::performQuery($query);
    }
    
    /**
     * 
     * {@inheritDoc}
     * @see \exface\Core\DataConnectors\MsSqlConnector::performDisconnect()
     */
    protected function performDisconnect()
    {
        $this->runSql("EXEC sp_dropserver @server='EXFOLAP'");
        parent::performDisconnect();
    }

    /**
     * 
     * @return string
     */
    public function getOlapSrvproduct() : string
    {
        return $this->olapSrvproduct;
    }

    /**
     * Custom srvproduct property of the linked server (empty strin by default).
     * 
     * @uxon-property olap_srvproduct
     * @uxon-type string
     * 
     * @param string $olapSrvproduct
     * @return MsSqlMdxConnector
     */
    public function setOlapSrvproduct(string $olapSrvproduct) : MsSqlMdxConnector
    {
        $this->olapSrvproduct = $olapSrvproduct;
        return $this;
    }

    /**
     * 
     * @return string
     */
    public function getOlapDatasrc() : string
    {
        return $this->olapDatasrc;
    }

    /**
     * The name of the data source to be used.
     * 
     * @uxon-property olap_datasrc
     * @uxon-type string
     * 
     * @param string $olapDatasrc
     * @return MsSqlMdxConnector
     */
    public function setOlapDatasrc(string $olapDatasrc) : MsSqlMdxConnector
    {
        $this->olapDatasrc = $olapDatasrc;
        return $this;
    }

    /**
     * 
     * @return string
     */
    public function getOlapCatalog() : string
    {
        return $this->olapCatalog;
    }

    /**
     * The name of the catalog to be used.
     * 
     * @uxon-property olap_catalog
     * @uxon-type string
     * 
     * @param string $olapCatalog
     * @return MsSqlMdxConnector
     */
    public function setOlapCatalog(string $olapCatalog) : MsSqlMdxConnector
    {
        $this->olapCatalog = $olapCatalog;
        return $this;
    }

    /**
     * 
     * @return string
     */
    public function getOlapProvider() : string
    {
        return $this->olapProvider;
    }

    /**
     * Custom provider for the linked server (instead of MSOLAP by default).
     * 
     * @uxon-property olap_provider
     * @uxon-type string
     * @uxon-default MSOLAP
     * 
     * @param string $olapProvider
     * @return MsSqlMdxConnector
     */
    public function setOlapProvider(string $olapProvider) : MsSqlMdxConnector
    {
        $this->olapProvider = $olapProvider;
        return $this;
    }
    
    /**
     * 
     * @param string $mdx
     * @return string
     */
    protected function buildSqlWrapper(string $mdx) : string
    {
        return <<<SQL
SELECT * FROM OPENQUERY(EXFOLAP,
'
{$mdx}
')   
SQL;
    }

}
?>