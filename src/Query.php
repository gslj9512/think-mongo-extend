<?php
// +----------------------------------------------------------------------
// | ThinkPHP [ WE CAN DO IT JUST THINK ]
// +----------------------------------------------------------------------
// | Licensed ( http://www.apache.org/licenses/LICENSE-2.0 )
// +----------------------------------------------------------------------
// | Author: gslj <gs19951211@gmail.com>
// +----------------------------------------------------------------------

namespace gslj\mongo;

use MongoDB\Driver\Command;
use think\exception\DbException;
use think\mongo\Query as BaseQuery;

class Query extends BaseQuery
{
    /**
     * @var array 管道
     */
    protected $pipeline = [];

    /**
     * 连表查询
     * @param $listRows
     * @return mixed
     */
    public function joinPage($listRows)
    {
        $page = request()->param('page', 1, 'intval');
        $page = $page >= 1 ? $page - 1 : $page;
        $limit = isset($listRows) ? $listRows : 16;
        $this->pipeline[] = [
            '$skip' => $page * $limit
        ];
        $this->pipeline[] = [
            '$limit' => $limit
        ];
        $cmd = [
            'aggregate' => $this->getTable(),
            'pipeline' => $this->pipeline,
            'explain' => false,
        ];

        return [
            'total' => $this->count(),
            'data' => $this->command(new Command($cmd)),
            'current_page' => $page
        ];
    }

    /**
     * 删除当前集合
     * @return mixed
     */
    public function drop()
    {
        $cmd = [
            'drop' => $this->getTable()
        ];
        return $this->command(new Command($cmd));
    }

    /**
     * 重命名集合
     * @param $name
     * @return mixed
     */
    public function rename($name)
    {
        //todo 需要重新建立索引
        $cmd = [
            'renameCollection' => $this->getConfig('database') . '.' . $this->getTable(),
            'to' => $this->getConfig('database') . '.' . $name
        ];
        return $this->command(new Command($cmd));
    }

    /**
     * 直接执行Command指令
     * @param $pipeline
     * @return mixed
     */
    public function joinCommand($pipeline)
    {
        $cmd = [
            'aggregate' => $this->getTable(),
            'pipeline' => $pipeline,
            'explain' => false,
        ];
        return $this->command(new Command($cmd));
    }

    public function joinMongo($join, $condition = null)
    {
        if (!isset($condition)) {
            throw new DbException('Fields must be specified for table connection operations');
        }
        $condition_array = explode('=', $condition);
        $look_up = [
            '$lookup' => [
                'from' => $join,
                'localField' => $condition_array[0],
                'foreignField' => '_id',
                'as' => $condition_array[0] . '_join'
            ]
        ];
        $unwind = [
            '$unwind' => [
                'path' => '$' . $condition_array[0] . '_join',
                "preserveNullAndEmptyArrays" => true
            ]
        ];
        $addFields = [
            '$addFields' => [
                $condition_array[0] => [
                    'name' => '$' . $condition_array[0] . '_join.' . $condition_array[1],
                    'id' => '$' . $condition_array[0] . '_join._id'
                ],
            ],
        ];
        $project = [
            '$project' => [
                $condition_array[0] . '_join' => 0
            ]
        ];
        $this->pipeline[] = $look_up;
        $this->pipeline[] = $unwind;
        $this->pipeline[] = $addFields;
        $this->pipeline[] = $project;
        return $this;
    }
}
