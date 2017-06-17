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
	 * @var \Tian\SqlBuild\MysqlBuild
	 */
	private $sqlBuild;
	public function __construct(\Tian\MysqlTableReflection $ref) {
		$this->tableReflection = $ref;
		$this->connection = $ref->connection;
		$this->cache = $ref->cache;
	}
	public function table($tab) {
		$this->tableReflection->setTableName ( $tab );
		$this->sqlBuild = new \Tian\SqlBuild\MysqlBuild ( $tab );
		return $this;
	}
	/**
	 * where(2)
	 * where("tb1.id = 10");
	 * where('name','Laravel-Academy')
	 * where("tb1.name = concat('dd',:lol,'gg') and tb1.id > :id",['lol' => 'lol_value','id' => 20])
	 * where('id', '>', 3)
	 *
	 * @return [expr,bind]
	 */
	protected function parseWhere($arg) {
		$num = count ( $arg );
		switch ($num) {
			case 1 :
				if (is_numeric ( $arg [0] )) {
					$pk = $this->tableReflection->getPk ();
					if (count ( $pk ) != 1) {
						throw new \Exception ( "表{$this->tableReflection->getTableName()}主键不为一，不能使用一个数字的WHERE" );
					}
					return [ 
							$pk,
							$arg [0] 
					];
				} else if (is_string ( $arg [0] )) {
					return [ 
							$arg [0],
							[ ] 
					];
				}
				throw new \Exception ( "WHERE条件只有一个，必须是数字" );
			case 2 :
				if (is_array ( $arg [1] )) {
					foreach ( $arg [1] as $k => $v ) {
						$this->sqlBuild->bindValue ( $k, $v );
					}
				}
				return [ 
						"`$arg [0]`=:$arg [0]",
						is_array ( $arg [1] ) ? $arg [1] : [ 
								$arg [0] => $arg [1] 
						] 
				];
			default :
				return [ 
						implode ( "", $arg ),
						[ ] 
				];
		}
	}
	/**
	 * where(2)
	 * where("tb1.id = 10")
	 * where('name','Laravel-Academy')
	 * where("tb1.name = concat('dd',:lol,'gg') and tb1.id > :id",['lol' => 'lol_value','id' => 20])
	 * where('id', '>', 3)
	 *
	 * @see \Tian\Db\MysqlDb::parseWhere
	 * @return \Tian\Db\MysqlDb
	 */
	public function where() {
		$w = $this->parseWhere ( func_get_args () );
		$this->sqlBuild->bindWhere ( $w [0], $w [1] );
		return $this;
	}
	/**
	 * orWhere(2)
	 * orWhere("tb1.id = 10")
	 * orWhere('name','Laravel-Academy')
	 * orWhere("tb1.name = concat('dd',:lol,'gg') and tb1.id > :id",['lol' => 'lol_value','id' => 20])
	 * orWhere('id', '>', 3)
	 *
	 * @see \Tian\Db\MysqlDb::parseWhere
	 * @return \Tian\Db\MysqlDb
	 */
	public function orWhere() {
		$w = $this->parseWhere ( func_get_args () );
		$this->sqlBuild->bindWhere ( $w [0], $w [1] );
		$this->sqlBuild->orWhere ();
		return $this;
	}
	
	/**
	 * whereBetween('votes', [1, 100])
	 * whereBetween('votes', [':start' => 1, ':end' => 100])
	 *
	 * @see \Tian\Db\MysqlDb::parseWhere
	 * @return \Tian\Db\MysqlDb
	 */
	public function whereBetween($field, array $bind) {
		// 判断是那种形式
		$k = array_keys ( $bind );
		if (preg_match ( "/^:\w+$/", $k [0] ) && preg_match ( "/^:\w+$/", $k [1] )) {
			$this->sqlBuild->bindWhere ( "`$field` BETWEEN " . $k [0] . " AND " . $k [1] . "" );
			$this->sqlBuild->bindValue ( $bind );
		} else if (! \Tian\Base\Arr::isAssoc ( $bind )) {
			$this->sqlBuild->bindWhere ( "`$field` BETWEEN " . $bind [0] . " AND " . $bind [1] . "" );
		}
		
		return $this;
	}
	/**
	 * whereNotBetween('votes', [1, 100])
	 * whereNotBetween('votes', [':start' => 1, ':end' => 100])
	 *
	 * @see \Tian\Db\MysqlDb::parseWhere
	 * @return \Tian\Db\MysqlDb
	 */
	public function whereNotBetween($field, array $bind) {
		// 判断是那种形式
		$k = array_keys ( $bind );
		if (preg_match ( "/^:\w+$/", $k [0] ) && preg_match ( "/^:\w+$/", $k [1] )) {
			$this->sqlBuild->bindWhere ( "`$field` NOT BETWEEN " . $k [0] . " AND " . $k [1] . "" );
			$this->sqlBuild->bindValue ( $bind );
		} else if (! \Tian\Base\Arr::isAssoc ( $bind )) {
			$this->sqlBuild->bindWhere ( "`$field` NOT BETWEEN " . $bind [0] . " AND " . $bind [1] . "" );
		}
		
		return $this;
	}
	/**
	 * whereIn('id', [1, 2, 3])
	 *
	 * @param string $field        	
	 * @param array $in        	
	 * @return \Tian\Db\MysqlDb
	 */
	public function whereIn($field, array $in) {
		$this->sqlBuild->bindWhere ( "`$field` IN (" . implode ( ",", $in ) . ")" );
		return $this;
	}
	/**
	 * whereNotIn('id', [1, 2, 3])
	 *
	 * @param string $field        	
	 * @param array $in        	
	 * @return \Tian\Db\MysqlDb
	 */
	public function whereNotIn($field, array $in) {
		$this->sqlBuild->bindWhere ( "`$field` NOT IN (" . implode ( ",", $in ) . ")" );
		return $this;
	}
	/**
	 *
	 * @param string $field        	
	 * @return \Tian\Db\MysqlDb
	 */
	public function whereNull($field) {
		$this->sqlBuild->bindWhere ( "`$field` IS NULL" );
		return $this;
	}
	/**
	 *
	 * @param string $field        	
	 * @return \Tian\Db\MysqlDb
	 */
	public function whereNotNull($field) {
		$this->sqlBuild->bindWhere ( "`$field` IS NOT NULL" );
		return $this;
	}
	/**
	 * select('id','name')
	 *
	 * @return \Tian\Db\MysqlDb
	 */
	public function select() {
		$args = func_get_args ();
		foreach ( $args as $field ) {
			$this->sqlBuild->bindField ( $field );
		}
		return $this;
	}
	/**
	 * orderBy('name', 'desc')
	 *
	 * @param string $expr        	
	 * @param string $ord        	
	 * @return \Tian\Db\MysqlDb
	 */
	public function orderBy($expr, $ord = 'DESC') {
		$this->sqlBuild->bindOrderBy ( $expr . " " . $ord );
		return $this;
	}
	/**
	 * groupBy('count')
	 *
	 * @param string $expr        	
	 * @return \Tian\Db\MysqlDb
	 */
	public function groupBy($expr) {
		$this->sqlBuild->bindGroupBy ( $expr );
		return $this;
	}
	/**
	 * having('count > 100')
	 * having('count > :c',['c' => 100])
	 * having('count', '>', 100)
	 * having('count', '>', ['c' => 100]) === having('count > :c',['c' => 100])
	 *
	 * @param string $expr        	
	 * @param string|array $oper        	
	 * @param string|array $bind        	
	 * @return \Tian\Db\MysqlDb
	 */
	public function having($expr, $oper = null, $bind = null) {
		if (is_null ( $oper ))
			$this->sqlBuild->bindHaving ( $expr );
		else if (is_array ( $oper ))
			$this->sqlBuild->bindHaving ( $expr, $oper );
		else if (is_string ( $oper ))
			if (is_array ( $bind )) {
				if (count ( $bind ) == 1 && \Tian\Base\Arr::isAssoc ( $bind ))
					$this->sqlBuild->bindHaving ( $expr . $oper . ':' . key ( $bind ), $bind );
			} else if (is_numeric ( $bind ) || is_string ( $bind )) {
				$this->sqlBuild->bindHaving ( $expr . $oper . $bind );
			}
		return $this;
	}
	/**
	 *
	 * @param number $offset        	
	 * @return \Tian\Db\MysqlDb
	 */
	public function skip($offset = 0) {
		$this->sqlBuild->bindLimit ( $offset );
		return $this;
	}
	/**
	 *
	 * @param number $length        	
	 * @return \Tian\Db\MysqlDb
	 */
	public function take($length = 10) {
		$this->sqlBuild->bindLimit ( $length );
		return $this;
	}
	/**
	 * 从数据表中取得单一数据列
	 */
	public function first() {
		$sql = $this->sqlBuild->select ();
		return $this->connection->fetch ( $sql, $this->sqlBuild->getBindValue () );
	}
	/**
	 * 从数据表中取得单一数据列的单一字段
	 * 失败返回null
	 */
	public function pluck($field) {
		$this->sqlBuild->bindField ( $field );
		$sql = $this->sqlBuild->select ();
		$row = $this->connection->fetch ( $sql, $this->sqlBuild->getBindValue () );
		if (isset ( $row [$field] ))
			return $row [$field];
		return null;
	}
	/**
	 * 从数据表中取得所有的数据列
	 * 失败返回[]
	 */
	public function get() {
		$sql = $this->sqlBuild->select ();
		return $this->connection->fetchAll ( $sql, $this->sqlBuild->getBindValue () );
	}
	/**
	 * 从数据表中分块查找数据列
	 * 通过在 闭包 中返回 false 来停止处理接下来的数据列
	 */
	public function chunk($size, \Closure $callback) {
		$i = 0;
		$offset = 0;
		$length = $size;
		while ( true ) {
			$sql = $this->sqlBuild->bindLimit ( ":offset,:length", [ 
					"offset" => $i * $length,
					"length" => $length 
			] )->select ();
			$data = $this->connection->fetchAll ( $sql, $this->sqlBuild->getBindValue () );
			if (! $data)
				break;
			foreach ( $data as $row ) {
				if ($callback ( $row ) === false)
					break;
			}
			$i ++;
		}
		return $this;
	}
	/**
	 * 取得单一字段值的列表DB::table('roles')->lists('title');
	 * 为返回的数组指定自定义键值。DB::table('roles')->lists('title', 'name');
	 */
	public function lists($filed, $key = null) {
		$data = $this->get ();
		return \Tian\Base\Arr::column ( $data, $filed, $key );
	}
}