# Milvus 错误处理（Error Handling）开发指南

## 1. 概述

为了提升 Milvus 系统的可观测性、简化排查流程，并为 SDK 提供统一的错误响应，项目引入了基于 `pkg/util/merr` 包的标准错误处理框架。

其核心目标是：
*   **统一错误码**：确保所有错误都关联一个明确的 Milvus 错误码（如 `ErrParameterInvalid`）。
*   **错误分类**：区分 **Input Error**（用户输入/客户端问题）和 **System Error**（系统内部异常）。
*   **错误链追踪**：保留从底层到顶层的完整错误堆栈和上下文信息。

---

## 2. 核心原则：防腐层与透传

在编写代码时，请遵循以下核心准则：

### A. 边界拦截（Boundary Calls）—— **必须翻译**
当你调用 **外部包**（如 Go 标准库 `json`、`os` 或第三方库 `etcd`、`rocksdb`、`pulsar`）时，它们返回的是原生 `error`。
*   **要求**：必须在调用处立即将其包装为 Milvus 标准错误。
*   **推荐函数**：使用 `merr.WrapErrXxxErr(err, ...)`。

```go
// 错误示例：直接返回第三方错误
if err := json.Unmarshal(data, &obj); err != nil {
    return err // ❌ 严禁直接返回原生错误
}

// 正确示例：翻译为 Milvus 领域错误
if err := json.Unmarshal(data, &obj); err != nil {
    return merr.WrapErrParameterInvalidErr(err, "fail to parse collection schema") // ✅
}
```

### B. 内部传播（Internal Propagation）—— **只追加上下文**
如果你调用的函数已经是 Milvus 内部函数且遵循本规范（返回的是 `merr`），你不需要再次用特定的错误码包装它。
*   **要求**：如果只是为了透传并添加一点当前的上下文信息（如 ID、名称），请使用通用的包装函数。
*   **推荐函数**：使用 `merr.Wrap(err, "context info")` 或 `merr.Wrapf(err, "...", args)`。
*   **原理**：这样可以保留底层的原始错误码（如 1005），同时在错误信息中追加当前的执行上下文。

```go
// 正确示例：透传并追加上下文，不改变底层错误性质
res, err := m.internalManager.GetSegment(segmentID)
if err != nil {
    return merr.Wrapf(err, "failed to get segment for collection %d", collectionID) // ✅ 依然保留底层的 ErrorCode
}
```

---

## 3. 常用函数指南

| 场景 | 推荐函数 | 说明 |
| :--- | :--- | :--- |
| **直接抛出新错误** | `merr.WrapErrXxxMsg("reason")` | 当检测到非法逻辑，直接返回一个带说明的标准错误。 |
| **包装底层原生 error** | `merr.WrapErrXxxErr(err, "format", args)` | 将外部产生的 `error` 归化为指定的 Milvus 错误码，并保留原错。 |
| **单纯透传并加上下文** | `merr.Wrap(err, "msg")` | **最常用**。用于内部调用链，保留底层错误码，仅追加调试信息。 |
| **显式标记错误类型** | `merr.WrapErrAsInputError(err)` | 强制将一个错误标记为用户输入错误（用于监控分类）。 |

---

## 4. 错误分类：System vs Input

在 `merr` 定义中，我们对错误进行了分类：
*   **Input Error**：由客户端行为引起（如：参数格式错误、集合不存在、权限不足）。
    *   *处理*：通常不触发报警，直接通过 SDK 返回给用户。
*   **System Error**：系统内部故障（如：磁盘 IO 失败、网络超时、内存不足、逻辑 Bug）。
    *   *处理*：会触发监控指标异常，需要开发人员介入排查。

**开发者职责**：在包装错误时，应根据业务语义选择合适的 `Wrap` 函数。例如，如果是检查用户传参失败，务必使用 `merr.ErrParameterInvalid` 相关的包装器。

---

## 5. 如何判断错误类型（Best Practices）

在代码中如果需要针对特定错误做逻辑分支，请始终使用 `merr.Is` 或 `errors.Is`：

```go
err := SomeFunction()
if merr.Is(err, merr.ErrCollectionNotFound) {
    // 针对集合不存在的特定补救逻辑
}

// 或者是检查大类
if merr.GetErrorType(err) == merr.InputError {
    // 记录审计日志，但不触发系统报警
}
```

---

## 6. 常见口子“补丁”：API 层的自动归化

为了防止漏网的原生 `error`（未经过 `merr` 包装）直接泄漏给客户端，我们在 API 顶层（Interceptor/Middleware）设置了兜底机制：

1.  **检测**：如果返回的 `error` 链条中不包含任何已定义的 Milvus 常量错误。
2.  **报警**：在日志中记录 "Leaked unhandled native error"。
3.  **归化**：自动将其包装为 `ErrServiceInternal` (Code 1) 返回给客户端，确保 API 结构的严谨性。

---

## 7. 总结：三步走写出优雅的错误处理

1.  **查底层**：如果底层是标准库/第三方库，用 `merr.WrapErrXxxErr`。
2.  **看当前**：如果底层已经是 `merr`，只需要 `merr.Wrap` 补全当前函数的上下文。
3.  **想用户**：返回的错误码是否能准确告诉用户该“改参数”还是“联系管理员”？