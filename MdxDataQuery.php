<?php
namespace exface\OlapDataConnector;

use exface\Core\CommonLogic\DataQueries\AbstractDataQuery;
use exface\Core\Factories\WidgetFactory;
use exface\Core\Widgets\DebugMessage;

class MdxDataQuery extends AbstractDataQuery
{
    private $mdx = null;
    
    private $resultArray = [];
    
    public function __construct(string $mdx)
    {
        $this->mdx = $mdx;
    }
    
    public function getMdx() : string
    {
        return $this->mdx;
    }
    
    public function getResultArray() : array
    {
        return $this->resultArray;
    }

    public function setResultArray(array $data) : MdxDataQuery
    {
        $this->resultArray = $data;
        return $this;
    }
    
    public function toString()
    {
        return $this->getMdx();
    }
    
    /**
     *
     * The MDX query creates a debug panel showing a formatted SQL statement.
     *
     * @see \exface\Core\CommonLogic\DataQueries\AbstractDataQuery::createDebugWidget()
     */
    public function createDebugWidget(DebugMessage $debug_widget)
    {
        $page = $debug_widget->getPage();
        $sql_tab = $debug_widget->createTab();
        $sql_tab->setCaption('MDX');
        $sql_tab->setNumberOfColumns(1);
        /* @var $sql_widget \exface\Core\Widgets\Html */
        $sql_widget = WidgetFactory::create($page, 'Html', $sql_tab);
        $sql_widget->setHtml('<pre style="padding:10px;">' . $this->getMdx() . '</pre>');
        $sql_widget->setWidth('100%');
        $sql_tab->addWidget($sql_widget);
        $debug_widget->addTab($sql_tab);
        return $debug_widget;
    }
}