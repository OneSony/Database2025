#include "include/query_engine/planner/operator/join_physical_operator.h"

/* TODO [Lab3] join的算子实现，需要根据join_condition实现Join的具体逻辑，
  最后将结果传递给JoinTuple, 并由current_tuple向上返回
 JoinOperator通常会遵循下面的被调用逻辑：
 operator.open()
 while(operator.next()){
    Tuple *tuple = operator.current_tuple();
 }
 operator.close()
*/

JoinPhysicalOperator::JoinPhysicalOperator() = default;

// 执行next()前的准备工作, trx是之后事务中会使用到的，这里不用考虑
RC JoinPhysicalOperator::open(Trx *trx)
{
  //左表遍历, 提前打开
  PhysicalOperator *left_node = children_[0].get();
  RC rc = left_node->open(trx);
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Failed to open left child operator");
    return rc;
  }

  //右表遍历, 提前打开
  PhysicalOperator *right_node = children_[1].get();
  rc = right_node->open(trx);
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Failed to open right child operator");
    return rc;
  }

  return RC::SUCCESS;
}

// 计算出接下来需要输出的数据，并将结果set到join_tuple中
// 如果没有更多数据，返回RC::RECORD_EOF
RC JoinPhysicalOperator::next()
{

  PhysicalOperator *left_node = children_[0].get();
  PhysicalOperator *right_node = children_[1].get();

  while(true) {
    // 先从左表获取数据
    RC rc = left_node->next();
    if (rc == RC::RECORD_EOF) {
      return rc;
    }
    joined_tuple_.set_left(left_node->current_tuple());

    while(true) {
      // 从右表获取数据
      rc = right_node->next();
      if (rc == RC::RECORD_EOF) {
        // 右表遍历完了, 切换下一个左表
        rc = right_node->close();
        if (rc != RC::SUCCESS) {
          LOG_ERROR("Failed to close right child operator");
          return rc;
        }
        rc = right_node->open(trx_);
        if (rc != RC::SUCCESS) {
          LOG_ERROR("Failed to open right child operator");
          return rc;
        }
        break;
      }
      joined_tuple_.set_right(right_node->current_tuple());

      //判断是否满足连接条件
      bool filter_result = false;
      rc = filter(joined_tuple_, filter_result);
      if (rc != RC::SUCCESS) {
        return rc;
      }

      if (filter_result) {
        // 当前结果满足条件, 返回
        return RC::SUCCESS;
      }
      //不满足继续遍历
    }
  }

}

// 节点执行完成，清理左右子算子
RC JoinPhysicalOperator::close()
{
  //左表EOF的时候会close, 此时右表刚开, 所以两个都需要关闭

  RC rc = children_[0]->close();
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Failed to close left child operator");
    return rc;
  }

  rc = children_[1]->close();
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Failed to close right child operator");
    return rc;
  }
  return RC::SUCCESS;
}

Tuple *JoinPhysicalOperator::current_tuple()
{
  return &joined_tuple_;
}

//TODO 对吗???
RC JoinPhysicalOperator::filter(JoinedTuple &tuple, bool &result)
{

  if(predicate_ == nullptr) {
    result = true;
    return RC::SUCCESS;
  }

  RC rc = RC::SUCCESS;
  Value value;
  
  rc = predicate_->get_value(tuple, value);
  if (rc != RC::SUCCESS) {
    return rc;
  }

  bool tmp_result = value.get_boolean();
  if (!tmp_result) {
    result = false;
    return rc;
  }

  result = true;
  return rc;
}
