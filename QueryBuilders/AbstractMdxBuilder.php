<?php
namespace exface\OlapDataConnector\QueryBuilders;

use exface\Core\CommonLogic\QueryBuilder\AbstractQueryBuilder;
use exface\Core\Interfaces\Model\MetaAttributeInterface;
use exface\Core\Interfaces\Model\MetaObjectInterface;
use exface\Core\Exceptions\QueryBuilderException;
use exface\Core\CommonLogic\AbstractDataConnector;
use exface\OlapDataConnector\MdxDataQuery;
use exface\Core\CommonLogic\QueryBuilder\QueryPartAttribute;
use exface\Core\DataTypes\StringDataType;
use exface\Core\CommonLogic\QueryBuilder\QueryPartFilter;
use exface\Core\DataTypes\SortingDirectionsDataType;

abstract class AbstractMdxBuilder extends AbstractQueryBuilder
{
    const INDENT = '    ';
    
    private $resultRowCountTotal = 0;
    
    private $resultTotals = [];
    
    private $resultRows = [];
    
    private $filtersOnAxes = [];
    
    private $with = [];
    
    /**
     * 
     * {@inheritDoc}
     * @see \exface\Core\CommonLogic\QueryBuilder\AbstractQueryBuilder::read()
     */
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
        
        $addressMapping = [];
        foreach ($this->getAttributes() as $qpart) {
            if ($qpart->getDataAddress() !== $qpart->getAlias()) {
                $addressMapping[$qpart->getDataAddress()][] = $qpart->getAlias();
            }
        }
        
        $query = new MdxDataQuery($this->buildMdxSelect());
        $query = $data_connection->query($query);
        
        $this->resultRows = $query->getResultArray();
        if (! empty($addressMapping)) {
            foreach ($this->resultRows as $nr => $row) {
                foreach ($addressMapping as $dataAddress => $aliases) {
                    foreach ($aliases as $alias) {
                        $row[$alias] = $row[$dataAddress];
                    }
                }
                $this->resultRows[$nr] = $row;
            }
        }
        $cnt = count($this->resultRows);
        
        $this->resultRowCountTotal = $this->getOffset() + $cnt;
        
        return $cnt;
    }
    
    /**
     * 
     * @return string
     */
    protected function buildMdxSelect() : string
    {   
        $indent = self::INDENT;
        $rows = $this->buildMdxSelectRows($indent, $this->getLimit(), $this->getOffset());
        $columns = $this->buildMdxSelectColumns($indent);
        
        $wheres = $this->buildMdxSelectWheres();
        if (! empty($wheres)) {
            $whereClause = implode(",\n" . $indent, $wheres);
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
    
    /**
     * 
     * @param string $indent
     * @return string
     */
    protected function buildMdxSelectColumns($indent = '') : string
    {
        $selects = [];
        foreach ($this->getAttributes() as $qpart) {
            if (! $this->isMeasure($qpart)) {
                continue;
            }
            $selects = array_merge($selects, $this->buildMdxSelectClauses($qpart));  
        }
        
        if (! empty($selects)) {
            $selectsString = implode(",\n".$indent.self::INDENT, $selects);
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
    
    /**
     * 
     * @param string $indent
     * @param int $limit
     * @param int $offset
     * @return string
     */
    protected function buildMdxSelectRows($indent = '', int $limit = 0, int $offset = 0) : string
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
            
            $selects[] = implode(",\n".$indent, $qpartSelects);
        }
        
        if (! empty($selects)) {
            $setIndent = $indent.self::INDENT;
            $selectsString = implode(" *\n".$setIndent, array_unique($selects));
            $set = "{\n{$setIndent}{$selectsString}\n{$indent}}";
            if (count($selects) > 1) {
                $set = "NONEMPTY({$set})";
            }
            
            $set = $this->buildMdxSelectOrder($set, $this->getSorters());
            
            if ($limit > 0) {
                $set = $this->buildMdxSubset($set, $limit, $offset);
            }
        } else {
            $set = '{ }';
        }
        
        return $set;
    }
    
    /**
     * 
     * @param QueryPartAttribute $qpart
     * @param string $function
     * @param array $filters
     * @return array
     */
    protected function buildMdxSelectClauses(QueryPartAttribute $qpart, string $function = null, array $filters = null) : array
    {
        if (! empty($filters)) {
            return $this->buildMdxSelectMemberFilters($qpart->getDataAddress(), $filters);
        } else {
            return [$this->buildMdxSelectClause($qpart, $function)];
        }
    }
    
    /**
     * 
     * @param QueryPartAttribute $qpart
     * @param string $function
     * @return string
     */
    protected function buildMdxSelectClause(QueryPartAttribute $qpart, string $function = null) : string
    {
        return $qpart->getDataAddress() . ($function !== null ? '.' . $function : '');
    }
    
    /**
     * 
     * @param string $member
     * @param array $filters
     * @throws QueryBuilderException
     * @return array
     */
    protected function buildMdxSelectMemberFilters(string $member, array $filters) : array
    {
        $selects = [];
        foreach ($filters as $filter) {
            switch ($filter->getComparator()) {
                case EXF_COMPARATOR_IS:
                    $selects[] = "FILTER({$member}, Instr({$this->getDimensionOfMember($member)}.CurrentMember.Name, '{$filter->getCompareValue()}') > 0)";
                    break;
                case EXF_COMPARATOR_IS_NOT:
                    $selects[] = "FILTER({$member}, Instr({$this->getDimensionOfMember($member)}.CurrentMember.Name, '{$filter->getCompareValue()}') = 0)";
                    break;
                case EXF_COMPARATOR_GREATER_THAN:
                case EXF_COMPARATOR_GREATER_THAN_OR_EQUALS:
                case EXF_COMPARATOR_LESS_THAN:
                case EXF_COMPARATOR_LESS_THAN_OR_EQUALS:
                    $selects[] = "FILTER({$member}, {$member} {$filter->getComparator()} {$filter->getCompareValue()}))";
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
    
    /**
     * 
     * @return string
     */
    protected function buildMdxFrom() : string
    {
        return 'FROM ' . $this->getCube($this->getMainObject());
    }
    
    /**
     * 
     * @return string[]
     */
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
    
    /**
     * 
     * {@inheritDoc}
     * @see \exface\Core\CommonLogic\QueryBuilder\AbstractQueryBuilder::getResultTotalRows()
     */
    public function getResultTotalRows()
    {
        return $this->resultRowCountTotal;
    }
    
    /**
     * 
     * {@inheritDoc}
     * @see \exface\Core\CommonLogic\QueryBuilder\AbstractQueryBuilder::canReadAttribute()
     */
    public function canReadAttribute(MetaAttributeInterface $attribute): bool
    {
        try {
            $otherCube = $this->getCube($attribute->getObject());
        } catch (QueryBuilderException $e) {
            return false;
        }
        return strcasecmp($this->getCube($this->getMainObject()), $otherCube) === 0;
    }
    
    /**
     * 
     * {@inheritDoc}
     * @see \exface\Core\CommonLogic\QueryBuilder\AbstractQueryBuilder::getResultRows()
     */
    public function getResultRows()
    {
        return $this->resultRows;
    }
    
    /**
     * 
     * {@inheritDoc}
     * @see \exface\Core\CommonLogic\QueryBuilder\AbstractQueryBuilder::getResultTotals()
     */
    public function getResultTotals()
    {
        return $this->resultTotals;
    }
    
    /**
     * 
     * @param MetaObjectInterface $object
     * @return string
     */
    protected function getCube(MetaObjectInterface $object) : string
    {
        return $object->getDataAddress();
    }
    
    /**
     * 
     * @param QueryPartAttribute $qpart
     * @return bool
     */
    protected function isMeasure(QueryPartAttribute $qpart) : bool
    {
        return StringDataType::startsWith($qpart->getDataAddress(), '[Measures].');
    }
    
    /**
     * 
     * @param QueryPartFilter $qpart
     * @return string
     */
    protected function getDimensionOfFilter(QueryPartFilter $qpart) : string
    {
        return $qpart->getDataAddress();
    }
    
    /**
     * 
     * @param string $path
     * @return array
     */
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
    
    /**
     * 
     * @param string $mdx
     * @param int $limit
     * @param int $offset
     * @return string
     */
    protected function buildMdxSubset(string $mdx, int $limit, int $offset = 0) : string
    {
        return "SUBSET({$mdx}, {$offset}, {$limit})";
    }
    
    /**
     * 
     * @param string $member
     * @return string
     */
    protected function getDimensionOfMember(string $member) : string
    {
        return StringDataType::substringBefore($member, '.', '', false, true);
    }
    
    /**
     * 
     * @return string
     */
    protected function buildMdxWith() : string
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
    
    /**
     * 
     * @param string $statement
     * @return AbstractMdxBuilder
     */
    protected function addWith(string $statement) : AbstractMdxBuilder
    {
        $this->with[] = $statement;
        return $this;
    }
    
    /**
     * 
     * @param string $rowSet
     * @param array $sorterQparts
     * @return string
     */
    protected function buildMdxSelectOrder(string $rowSet, array $sorterQparts) : string
    {
        foreach ($sorterQparts as $qpart) {
            $dir = $qpart->getOrder() == SortingDirectionsDataType::ASC ? 'BASC' : 'BDESC';
            if ($this->isMeasure($qpart)) {
                $sortBy = $qpart->getDataAddress();
            } else {
                $sortBy = $this->getDimensionOfMember($qpart->getDataAddress()) . '.CurrentMember.Member_Name';
            }
            $rowSet = "ORDER({$rowSet}, {$sortBy}, {$dir})";
        }
        return $rowSet;
    }
}