<?php
namespace exface\OlapDataConnector\QueryBuilders;

use exface\Core\CommonLogic\QueryBuilder\AbstractQueryBuilder;
use exface\Core\Interfaces\Model\MetaAttributeInterface;
use exface\Core\Interfaces\Model\MetaObjectInterface;
use exface\Core\Exceptions\QueryBuilderException;
use exface\OlapDataConnector\MdxDataQuery;
use exface\Core\CommonLogic\QueryBuilder\QueryPartAttribute;
use exface\Core\DataTypes\StringDataType;
use exface\Core\CommonLogic\QueryBuilder\QueryPartFilter;
use exface\Core\DataTypes\SortingDirectionsDataType;
use exface\Core\Interfaces\DataSources\DataConnectionInterface;
use exface\Core\Interfaces\DataSources\DataQueryResultDataInterface;
use exface\Core\CommonLogic\DataQueries\DataQueryResultData;

/**
 * Generic MDX builder.
 * 
 * ## Data addresses of objects
 * 
 * Each cube should be represented by a single meta object. The data address is the cube name: e.g. `[My Cube]`.
 * 
 * Technically, it is possible to have multiple meta objects based on the same cube, but putting
 * attributes of these objects together in a single data widget will result in multiple MDX queries.
 * 
 * ## Data addresses of attributes
 * 
 * Data addresses for attributes are member expressions: e.g. `[Measures].[My Measure]`.
 * 
 * The query builder places attributes automatically on the respective axes, so there is no need to
 * specify, whether an attribute is a measure or a dimension.
 * 
 * ### Data address properties
 * 
 * - `MDX_MEMBER_PROPERTY` - allows to address a specific property of a member instead of the member 
 * name, which is used by default: e.g. [Dimension].[Dimension].CurrentMember.PROPERTIES["..."]. 
 * Possible values depend on the OLAP engine used. However, most will support `KEY`, so setting this 
 * option to `KEY` will select/filter member keys.
 * 
 * ## Known limitations
 * 
 * - No total count of rows available when using pagination. The problem is, that there is no efficient
 * way to calculate the number of non-empty values in an MDX query. Pagination can be easily done via
 * SUBSET(), but there is no way to find out, how many pages there are. 
 * 
 * @author Andrej Kabachnik
 *
 */
abstract class AbstractMdxBuilder extends AbstractQueryBuilder
{
    const INDENT = '    ';
    
    private $filtersOnAxes = [];
    
    private $with = [];
    
    private $resultColumnAliases = [];
    
    /**
     * 
     * {@inheritDoc}
     * @see \exface\Core\CommonLogic\QueryBuilder\AbstractQueryBuilder::read()
     */
    public function read(DataConnectionInterface $data_connection) : DataQueryResultDataInterface
    {
        // Increase limit by one to check if there are more rows
        $originalLimit = $this->getLimit();
        if ($originalLimit > 0) {
            $this->setLimit($originalLimit+1, $this->getOffset());
        }
        
        $query = new MdxDataQuery($this->buildMdxSelect());
        $query = $data_connection->query($query);
        
        $resultRows = $query->getResultArray();
        $aliasMappings = $this->getResultColumnAliases();
        if (! empty($aliasMappings)) {
            foreach ($resultRows as $nr => $row) {
                foreach ($aliasMappings as $columnName => $aliases) {
                    foreach ($aliases as $alias) {
                        $row[$alias] = $row[$columnName];
                    }
                }
                $resultRows[$nr] = $row;
            }
        }
        $cnt = count($resultRows);
        
        $hasMoreRows = ($originalLimit > 0 && $cnt === $originalLimit+1);
        if ($hasMoreRows === true) {
            $affectedCounter = $originalLimit;
            array_pop($resultRows);
        } else {
            $affectedCounter = $cnt;
        }
        
        if ($hasMoreRows === false) {
            $totalCount = $cnt + $this->getOffset();
        } else {
            $hasMoreRows = null;
        }
        
        return new DataQueryResultData($resultRows, $affectedCounter, $hasMoreRows, $totalCount);
    }
    
    /**
     * 
     * {@inheritDoc}
     * @see \exface\Core\CommonLogic\QueryBuilder\AbstractQueryBuilder::count()
     */
    public function count(DataConnectionInterface $data_connection) : DataQueryResultDataInterface
    {
        // TODO Counting the total amount of rows produced by an MDX statement beyond the current page
        // seems not so easy in MDX, so for the moment we just requenst limit+1 rows to tell the UI,
        // there is another page.
        
        $originalLimit = $this->getLimit();
        if ($originalLimit > 0) {
            $this->setLimit($originalLimit+1, $this->getOffset());
        }
        
        $query = new MdxDataQuery($this->buildMdxSelect());
        $query = $data_connection->query($query);
        
        $resultRows = $query->getResultArray();
        $cnt = count($resultRows);
        
        $hasMoreRows = ($cnt === $originalLimit+1);
        $totalCount = $cnt + $this->getOffset();
        
        return new DataQueryResultData([], $totalCount, $hasMoreRows, $totalCount);
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
            
            if ($qpart->getDataAddress() !== $qpart->getAlias()) {
                $this->addResultColumnAlias($qpart->getAlias(), $qpart->getDataAddress());
            }
        }
        
        foreach (array_keys($this->with) as $member) {
            $selects[] = $member;
            $measure = StringDataType::startsWith($member, '[Measures]') ? $member : "[Measures].[$member]";
            if ($measure !== $member) {
                $this->addResultColumnAlias($member, $measure);
            }
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
            
            // Check, if we are actually interested in the member name or another property
            if ($prop = $qpart->getDataAddressProperty('MDX_MEMBER_PROPERTY')) {
                // If a property must be selected, we need a calculated member.
                $member = $this->sanitizeMemberName($qpart->getAlias());
                $this->addWith($member, "MEMBER {$member} AS {$this->getDimensionOfMember($qpart->getDataAddress())}.CurrentMember.PROPERTIES(\"{$prop}\")");
            } else {
                // If the value itself is of interest, use the data address
                if ($qpart->getDataAddress() !== $qpart->getAlias()) {
                    $this->addResultColumnAlias($qpart->getAlias(), $qpart->getDataAddress());
                }
            }
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
            $property = $filter->getDataAddressProperty('MDX_MEMBER_PROPERTY') ? strtoupper($filter->getDataAddressProperty('MDX_MEMBER_PROPERTY')) : 'Name';
            switch ($filter->getComparator()) {
                case EXF_COMPARATOR_IS:
                    $selects[] = "FILTER({$member}, Instr({$this->getDimensionOfMember($member)}.CurrentMember.{$property}, '{$filter->getCompareValue()}') > 0)";
                    break;
                case EXF_COMPARATOR_IS_NOT:
                    $selects[] = "FILTER({$member}, Instr({$this->getDimensionOfMember($member)}.CurrentMember.{$property}, '{$filter->getCompareValue()}') = 0)";
                    break;
                case EXF_COMPARATOR_GREATER_THAN:
                case EXF_COMPARATOR_GREATER_THAN_OR_EQUALS:
                case EXF_COMPARATOR_LESS_THAN:
                case EXF_COMPARATOR_LESS_THAN_OR_EQUALS:
                    $selects[] = "FILTER({$member}, {$member} {$filter->getComparator()} {$filter->getCompareValue()}))";
                    break;
                case EXF_COMPARATOR_EQUALS:
                    switch ($property) {
                        case 'KEY' : $selects[] = $member . '.&[' . $filter->getCompareValue() . ']'; break;
                        case 'Name' : $selects[] = $member . '.[' . $filter->getCompareValue() . ']'; break;
                        default: throw new QueryBuilderException('Cannot build MDX equals-filter for "' . $filter->getAlias() . '": filtering over member property "' . $property . '" not supported!');
                    }
                    break;
                case EXF_COMPARATOR_IN:
                    $values = explode($filter->getAttribute()->getValueListDelimiter(), $filter->getCompareValue());
                    foreach ($values as $val) {
                        switch ($property) {
                            case 'KEY' : $selects[] = $member . '.&[' . $val . ']'; break;
                            case 'Name' : $selects[] = $member . '.[' . $val . ']'; break;
                            default: throw new QueryBuilderException('Cannot build MDX in-filter for "' . $filter->getAlias() . '": filtering over member property "' . $property . '" not supported!');
                        }
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
    protected function addWith(string $memberName, string $statement) : AbstractMdxBuilder
    {
        $this->with[$memberName] = $statement;
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
    
    protected function sanitizeMemberName(string $name) : string
    {
        return str_replace(['[', ']', ' '], '_', $name);
    }
    
    protected function getResultColumnAliases() : array
    {
        return $this->resultColumnAliases;
    }
    
    protected function addResultColumnAlias(string $alias, string $resultColumnName) : AbstractMdxBuilder
    {
        $this->resultColumnAliases[$resultColumnName][] = $alias;
        return $this;
    }
}