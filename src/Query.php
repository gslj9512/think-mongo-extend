<?php
// +----------------------------------------------------------------------
// | ThinkPHP [ WE CAN DO IT JUST THINK ]
// +----------------------------------------------------------------------
// | Licensed ( http://www.apache.org/licenses/LICENSE-2.0 )
// +----------------------------------------------------------------------
// | Author: liu21st <liu21st@gmail.com>
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
                'as' => $join
            ]
        ];
        $unwind = [
            '$unwind' => '$' . $join,
            "preserveNullAndEmptyArrays" => true
        ];
        $addFields = [
            '$addFields' => [
                $condition_array[0] => '$' . $join . '.' . $condition_array[1]
            ],
        ];
        $this->pipeline[] = $look_up;
        $this->pipeline[] = $unwind;
        $this->pipeline[] = $addFields;
        return $this;
    }
}
