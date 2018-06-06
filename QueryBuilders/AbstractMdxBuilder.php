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
use exface\Core\CommonLogic\QueryBuilder\QueryPartFilter;


abstract class AbstractMdxBuilder extends AbstractQueryBuilder
{
    const AXIS_COLUMNS = 0;
    const AXIS_ROWS = 1;
    const AXIS_WHERE = 'WHERE';
    
    private $resultRowCountTotal = 0;
    
    private $resultTotals = [];
    
    private $resultRows = [];
    
    private $filtersOnAxes = [];
    
    private $with = [];
    
    public function read(AbstractDataConnector $data_connection = null)
    {
        if ($data_connection === null) {
            $data_connection = $this->getMainObject()->getDataConnection();
        }
        
        // TODO Counting the total amount of rows produced by an MDX statement beyond the current page
        // seems not so easy in MDX, so for the moment we just requenst limit+1 rows to tell the UI,
        // there is another page.
        
        $originalLimit = $this->getLimit();
        $this->setLimit($originalLimit+1, $this->getOffset());
        
        $query = new MdxDataQuery($this->buildMdxSelect());
        $query = $data_connection->query($query);
        
        $this->resultRows = $query->getResultArray();
        $cnt = count($this->resultRows);
        
        $this->resultRowCountTotal = $this->getOffset() + $cnt;
        
        return $cnt;
    }
    
    protected function buildMdxSelect() : string
    {        
        $rows = $this->buildMdxSelectRows($this->getLimit(), $this->getOffset());
        $columns = $this->buildMdxSelectColumns();
        
        $wheres = $this->buildMdxSelectWheres();
        if (! empty($wheres)) {
            $whereClause = implode(",\n  ", $wheres);
            $where = <<<MDX
WHERE (
    {$whereClause}
)
MDX;
        }
        
        // Build the MDX
        // IMPORTANT: all parts, that can require a WITH statement, must be already built
        // at this point!!! This is why columns and rows are already rendered above and
        // saved into variables.
        $mdx = <<<MDX

{$this->buildMdxWith()}
SELECT 
    {$columns} ON COLUMNS,
    {$rows} ON ROWS
{$this->buildMdxFrom()}
{$where}

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
            $selects = array_merge($selects, $this->buildMdxSelectClauses($qpart));  
        }
        
        if (! empty($selects)) {
            $selectsString = implode(",\n    ", $selects);
            $set =  <<<MDX
NON EMPTY {
        {$selectsString}
    }
MDX;
        } else {
            $set = '{ }';
        }
        
        return $set;
    }
    
    protected function buildMdxSelectRows(int $limit = 0, int $offset = 0) : string
    {
        $selects = [];
        foreach ($this->getAttributes() as $qpart) {
            if ($this->isMeasure($qpart)) {
                continue;
            }
            $filters = [];
            foreach ($this->getFiltersForMember($qpart->getDataAddress()) as $filter) {
                $this->filtersOnAxes[] = $filter;
                if ($filter->getCompareValue() !== null && $filter->getCompareValue() !== '') {
                    $filters[] = $filter;
                }
                
            }
            
            if (empty($filters)) {
                $qpartSelects = $this->buildMdxSelectClauses($qpart, 'ALLMEMBERS');
            } else {
                $qpartSelects = $this->buildMdxSelectClauses($qpart, 'FILTER', $filters);
            }
            
            $selects[] = implode(",\n    ", $qpartSelects);
        }
        
        if (! empty($selects)) {
            $selectsString = implode(" *\n    ", $selects);
            $set .=  <<<MDX
{
        {$selectsString}
    }
MDX;
            if (count($selects) > 1) {
                $set = 'NONEMPTY(' . $set . ')';
            }
            
            if ($limit > 0) {
                $set = $this->buildMdxSubset($set, $limit, $offset);
            }
        } else {
            $set = '{ }';
        }
        
        return $set;
    }
    
    protected function buildMdxSelectClauses(QueryPartAttribute $qpart, string $function = null, array $filters = null) : array
    {
        if (! empty($filters)) {
            return $this->buildMdxSelectMemberFilters($qpart->getDataAddress(), $filters);
        } else {
            return [$this->buildMdxSelectClause($qpart, $function)];
        }
    }
    
    protected function buildMdxSelectClause(QueryPartAttribute $qpart, string $function = null) : string
    {
        return $qpart->getDataAddress() . ($function !== null ? '.' . $function : '');
    }
    
    protected function buildMdxSelectMemberFilters(string $member, array $filters) : array
    {
        $selects = [];
        foreach ($filters as $filter) {
            switch ($filter->getComparator()) {
                case EXF_COMPARATOR_IS:
                    $selects[] = "FILTER({$member}, Instr({$this->getDimensionOfMember($member)}.CurrentMember.Name, '{$filter->getCompareValue()}') > 0)";
                    break;
                case EXF_COMPARATOR_EQUALS:
                    $selects[] = $member . '.[' . $filter->getCompareValue() . ']';
                    break;
                case EXF_COMPARATOR_IN:
                    $values = explode($filter->getAttribute()->getValueListDelimiter(), $filter->getCompareValue());
                    foreach ($values as $val) {
                        $selects[] = $member . '.[' . $val . ']';
                    }
                    break;
                default:
                    throw new QueryBuilderException('Comparator ' . $filter->getComparator() . ' currently not supported in MDX queries!');
            }
        }
        return $selects;
    }
    
    
    
    protected function buildMdxFrom() : string
    {
        return 'FROM ' . $this->getCube($this->getMainObject());
    }
    
    protected function buildMdxSelectWheres() : array
    {
        $wheres = [];
        foreach ($this->getFilters()->getFilters() as $filter) {
            if (in_array($filter, $this->filtersOnAxes)) {
                continue;
            }
            
            $wheres = array_merge($wheres, $this->buildMdxSelectMemberFilters($filter->getDataAddress(), [$filter]));
        }
        return $wheres;
    }
    
    public function getResultTotalRows()
    {
        return $this->resultRowCountTotal;
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
    
    protected function getDimensionOfFilter(QueryPartFilter $qpart) : string
    {
        return $qpart->getDataAddress();
    }
    
    protected function getFiltersForMember(string $path) : array
    {
        $result = [];
        foreach ($this->getFilters()->getFilters() as $qpart) {
            if ($path === $qpart->getDataAddress()) {
                $result[] = $qpart;
            }
        }
        return $result;
    }
    
    protected function buildMdxSubset(string $mdx, int $limit, int $offset = 0) : string
    {
        return "SUBSET({$mdx}, {$offset}, {$limit})";
    }
    
    protected function getDimensionOfMember(string $member) : string
    {
        return StringDataType::substringBefore($member, '.', '', false, true);
    }
    
    protected function buildMdxWith()
    {
        if (! empty($this->with)) {
            $withs = implode("\n  ", $this->with);
            return <<<MDX
WITH
    {$withs}
MDX;
        }
        
        return '';
    }
    
    protected function addWith(string $statement) : AbstractMdxBuilder
    {
        $this->with[] = $statement;
        return $this;
    }
    
    protected function hasPagination()
    {
        return $this->getLimit() > 0;
    }
}