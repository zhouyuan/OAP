#pragma once

#include <arrow/status.h>
#include <gandiva/node.h>
#include <gandiva/node_visitor.h>

namespace sparkcolumnarplugin {
namespace codegen {
class VisitorBase : public gandiva::NodeVisitor {
  arrow::Status Visit(const gandiva::FunctionNode& node) override {
    return arrow::Status::OK();
  }
  arrow::Status Visit(const gandiva::FieldNode& node) override {
    return arrow::Status::OK();
  }
  arrow::Status Visit(const gandiva::IfNode& node) override {
    return arrow::Status::OK();
  }
  arrow::Status Visit(const gandiva::LiteralNode& node) override {
    return arrow::Status::OK();
  }
  arrow::Status Visit(const gandiva::BooleanNode& node) override {
    return arrow::Status::OK();
  }
  arrow::Status Visit(const gandiva::InExpressionNode<int>& node) override {
    return arrow::Status::OK();
  }
  arrow::Status Visit(const gandiva::InExpressionNode<long int>& node) override {
    return arrow::Status::OK();
  }
  arrow::Status Visit(const gandiva::InExpressionNode<std::string>& node) override {
    return arrow::Status::OK();
  }
};
}  // namespace codegen
}  // namespace sparkcolumnarplugin
