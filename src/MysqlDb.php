<?php

/**
 * @date: 2017/5/31 10:47:23
 * @author: awei.tian
 * @mysql CRUD快速操作
 */
namespace Tian\Db;

class MysqlDb {
	/**
	 *
	 * @var \Tian\Connection\MysqlPdoConn
	 */
	public $connection;
	/**
	 *
	 * @var \Tian\ICache
	 */
	public $cache;
	
	/**
	 *
	 * @var \Tian\MysqlTableReflection
	 */
	public $tableReflection;
	/**
	 *
	 * @var \Tian\MysqlTableReflection
	 */
	private $table;
	/**
	 *
	 * @var \Tian\SqlBuild\MysqlDeleteBuild
	 */
	private $deleteBuild;
	/**
	 *
	 * @var \Tian\SqlBuild\MysqlInsertBuild
	 */
	private $inserteBuild;
	/**
	 *
	 * @var \Tian\SqlBuild\MysqlReplaceBuild
	 */
	private $replaceBuild;
	/**
	 *
	 * @var \Tian\SqlBuild\MysqlSelectBuild
	 */
	private $selectBuild;
	/**
	 *
	 * @var \Tian\SqlBuild\MysqlUpdateBuild
	 */
	private $updateBuild;
	/**
	 * 当前处于CURD哪种模式
	 * 
	 * @var string
	 */
	private $mode;
	public function __construct(\Tian\MysqlTableReflection $ref) {
		$this->tableReflection = $ref;
		$this->connection = $ref->connection;
		$this->cache = $ref->cache;
	}
	public function prepareDelete($tab) {
		$this->mode = 'delete';
		$this->tableReflection->setTableName ( $tab );
		$this->deleteBuild = new \Tian\SqlBuild\MysqlDeleteBuild ( $tab );
		return $this;
	}
	/**
	 * 如果是一个数字，主为是主键等于这个数字
	 * 如果是两个参数，则认为是FIELD = VALUE
	 * @return \Tian\Db\MysqlDb
	 */
	public function where() {
		switch ($this->mode) {
			case 'delete':
				return call_user_func_array([$this,'deleteWhere'], func_get_args ());
		}
	}
	/**
	 * 如果参数只是一个数字，则认为主键等于这个数字
	 * @return \Tian\Db\MysqlDb
	 */
	public function deleteWhere() {
		$num = func_num_args();
		$arg = func_get_args();
		switch ($num) {
			case 1:
				if(is_numeric($arg[0])){
					return $this->pkEqual($arg[0]);
				}
				throw new \Exception("WHERE条件只有一个，必须是数字");
			case 2:
				
		}
		return $this;
	}
	private function pkEqual($num) {
		$pk = $this->tableReflection->getPk();
		if (count($pk) != 1) {
			throw new \Exception("表{$this->tableReflection->getTableName()}主键不为一，不能使用一个数字的WHERE");
		}
		return $this->where($pk[0],$num);
	}
	private function fieldEqual($field,$val) {
		switch ($this->mode) {
			case 'delete':
				$this->deleteBuild->bindWhereExpr(sid>100)
		}
	}
}