#pragma once

#include "physical_operator.h"
#include "include/query_engine/structor/tuple/join_tuple.h"

// TODO [Lab3] join算子的头文件定义，根据需要添加对应的变量和方法
class JoinPhysicalOperator : public PhysicalOperator
{
public:
  JoinPhysicalOperator();
  ~JoinPhysicalOperator() override = default;

  PhysicalOperatorType type() const override
  {
    return PhysicalOperatorType::JOIN;
  }

  RC open(Trx *trx) override;
  RC next() override;
  RC close() override;
  Tuple *current_tuple() override;

  void set_predicate(std::unique_ptr<Expression> &&expr)
  {
    predicate_ = std::move(expr);
  }

private:
  RC filter(JoinedTuple &tuple, bool &result);

private:
  Trx *trx_ = nullptr;
  JoinedTuple joined_tuple_;  //! 当前关联的左右两个tuple
  std::unique_ptr<Expression> predicate_;
};
