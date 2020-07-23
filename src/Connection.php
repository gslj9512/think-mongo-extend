<?php
// +----------------------------------------------------------------------
// | ThinkPHP [ WE CAN DO IT JUST THINK ]
// +----------------------------------------------------------------------
// | Licensed ( http://www.apache.org/licenses/LICENSE-2.0 )
// +----------------------------------------------------------------------
// | Author: liu21st <liu21st@gmail.com>
// +----------------------------------------------------------------------

namespace gslj\mongo;

use think\mongo\Connection as BaseConnection;

/**
 * Mongo数据库驱动
 */
class Connection extends BaseConnection
{
    /**
     * 获得数据集
     * @access protected
     * @param bool|string       $class true 返回Mongo cursor对象 字符串用于指定返回的类名
     * @param string|array      $typeMap 指定返回的typeMap
     * @return mixed
     */
    protected function getResult($class = '', $typeMap = null)
    {
        if (true === $class) {
            return $this->cursor;
        }

        // 设置结果数据类型
        if (is_null($typeMap)) {
            $typeMap = $this->typeMap;
        }

        $typeMap = is_string($typeMap) ? ['root' => $typeMap] : $typeMap;

        $this->cursor->setTypeMap($typeMap);

        // 获取数据集
        $result = $this->cursor->toArray();

        if ($this->getConfig('pk_convert_id')) {
            // 转换ObjectID 字段
            foreach ($result as &$data) {
                $this->convertObjectID($data);
            }
        }

        $this->numRows = count($result);

        return $result;
    }

    /**
     * ObjectID处理
     * @access public
     * @param array     $data
     * @return void
     */
    private function convertObjectID(&$data)
    {
        if (isset($data['_id']) && is_object($data['_id'])) {
            $data['id'] = $data['_id']->__toString();
            unset($data['_id']);
        }

        foreach ($data as $key => &$datum) {

            if (is_object($datum)){
                $datum = $datum->__toString();
                continue;
            }

            if (isset($datum['_id'])
                && is_object($datum['_id'])){
                $datum['id'] = $datum['_id']->__toString();
                unset($datum['_id']);
            }

            if (is_array($datum)){
                foreach ($datum as $index => &$item) {
                    if (is_object($item)){
                        $item = $item->__toString();
                    }
                }
            }
        }
    }
}
