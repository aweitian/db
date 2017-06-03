# PDO Connection组件
## 安装组件
使用 composer 命令进行安装或下载源代码使用。
>composer require aweitian/db-connection
>
```
//参数错误出抛出异常
new Tian\Connection\MysqlPdoConn ( [ 
	'host' => '127.0.0.1',
	'port' => 3306,
	'user' => 'root',
	'password' => 'root',
	'charset' => 'utf8',
	'database' => 'garri' 
] );
```
## 设置调试模式
```
/**
 * 
 * 设置为TRUE。异常由PDO对象抛出
 * 默认为FALSE，异常由MysqlPdoConn对象抛出
 */
function setDebugMode($mode);
```
## 连接信息获取
```
getDbName();  #返回数据库名
getHost();    #返回主机名
getCharset(); #返回连接端口
```
## 添加记录
```
/**
 *
 * 当主键是自增时，返回插入ID,其它返回0
 * 有错误时会抛出异常
 * @param string $sql        	
 * @param array $data       eg: ['k' => 'v','a' => 'b']       	
 * @param array $bindType   KEY和DATA一样，值为PDO:PARAM_**
 * @return int
 */
insert($sql, $data = [], $bindType = []);
```
## 获取一行数据
```
/**
 *
 * 返回一维数组,SQL中的结果集中的第一个元组
 * 有错误时会抛出异常
 * @param string $sql        	
 * @param array $data        	
 * @return array;
 */
public function fetch($sql, $data = [], $bindType = [], $fetch_mode = \PDO::FETCH_ASSOC)
```
## 获取表格数据
```
/**
 *
 * 返回二维数组
 * 有错误时会抛出异常
 * @param string $sql        	
 * @param array $data        	
 * @return array;
 */
public function fetchAll($sql, $data = [], $bindType = [], $fetch_mode = \PDO::FETCH_ASSOC)
```
## SQL执行操作，可多条语句同时执行
```
/**
 *
 * 返回影响行数
 * 有错误时会抛出异常
 * @param string $sql        	
 * @param array $data        	
 * @param array $bindType
 *        	KEY和DATA一样，值为PDO:PARAM_**
 * @return int
 */
public function exec($sql, $data = [], $bindType = [])
```
## 事务
```
function transaction(\Closure $closure) #返回对象本身
function beginTransaction() #返回对象本身
function rollback()         #返回对象本身
function commit()           #返回对象本身
function getQueryLog()      #返回对象本身
```
