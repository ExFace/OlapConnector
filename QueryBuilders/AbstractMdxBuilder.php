<?php
namespace exface\OlapDataConnector\QueryBuilders;

use exface\Core\CommonLogic\QueryBuilder\AbstractQueryBuilder;
use exface\Core\Interfaces\Model\MetaAttributeInterface;
use exface\Core\Interfaces\Model\MetaObjectInterface;
use exface\Core\Exceptions\QueryBuilderException;
use exface\Core\CommonLogic\AbstractDataConnector;
use exface\OlapDataConnector\MdxDataQuery;
use exface\Core\CommonLogic\QueryBuilder\QueryPartAttribute;
use exface\Core\DataTypes\BooleanDataType;
use exface\Core\DataTypes\StringDataType;


abstract class AbstractMdxBuilder extends AbstractQueryBuilder
{
    private $resultTotalRows = 0;
    private $resultTotals = [];
    private $resultRows = [];
    
    public function read(AbstractDataConnector $data_connection = null)
    {
        if ($data_connection === null) {
            $data_connection = $this->getMainObject()->getDataConnection();
        }
        
        $query = new MdxDataQuery($this->buildMdxSelect());
        $query = $data_connection->query($query);
        
        $this->resultRows = $query->getResultArray();
        $cnt = count($this->resultRows);
        $this->resultTotalRows = $cnt;
        return $cnt;
    }
    
    protected function buildMdxSelect() : string
    {        
        $mdx = <<<MDX

SELECT 
    {$this->buildMdxSelectColumns()},
    {$this->buildMdxSelectRows()}
    {$this->buildMdxFrom()}
    {$this->buildMdxSelectWhere()}

MDX;
    
    return $mdx;
    
    }
        
    protected function buildMdxSelectColumns() : string
    {
        $selects = [];
        foreach ($this->getAttributes() as $qpart) {
            if (! $this->isMeasure($qpart)) {
                continue;
            }
            $selects[$qpart->getAlias()] = $this->buildMdxSelectClause($qpart);  
        }
        
        if (! empty($selects)) {
            $selectsString = implode(",\t\t\n", $selects);
            $set =  <<<MDX
    NON EMPTY {
        {$selectsString}
    }
MDX;
        } else {
            $set = '{ }';
        }
        
        return $set . ' ON COLUMNS';
    }
    
    protected function buildMdxSelectClause(QueryPartAttribute $qpart, string $function = null) : string
    {
        return $qpart->getDataAddress() . ($function !== null ? '.' . $function : '');
    }
    
    protected function buildMdxSelectRows() : string
    {
        $selects = [];
        foreach ($this->getAttributes() as $qpart) {
            if ($this->isMeasure($qpart)) {
                continue;
            }
            $selects[$qpart->getAlias()] = $this->buildMdxSelectClause($qpart, 'ALLMEMBERS');
        }
        
        if (! empty($selects)) {
            $selectsString = implode(",\n\t\t", $selects);
            $set =  <<<MDX
    {
        {$selectsString}
    }
MDX;
        } else {
            $set = '{ }';
        }
        
        return $set . ' ON ROWS';
    }
    
    protected function buildMdxFrom() : string
    {
        return 'FROM ' . $this->getCube($this->getMainObject());
    }
    
    protected function buildMdxSelectWhere() : string
    {
        return '';
    }
    
    public function getResultTotalRows()
    {
        return $this->resultRows;
    }
    
    public function canRead(MetaAttributeInterface $attribute): bool
    {
        try {
            $otherCube = $this->getCube($attribute->getObject());
        } catch (QueryBuilderException $e) {
            return false;
        }
        return strcasecmp($this->getCube($this->getMainObject()), $otherCube) === 0;
    }
    
    public function getResultRows()
    {
        return $this->resultRows;
    }
    
    public function getResultTotals()
    {
        return $this->resultTotals;
    }
    
    protected function getCube(MetaObjectInterface $object)
    {
        return $object->getDataAddress();
    }
    
    protected function isMeasure(QueryPartAttribute $qpart) : bool
    {
        return StringDataType::startsWith($qpart->getDataAddress(), '[Measures].');
    }
}