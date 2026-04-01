# Milvus Error Handling Guideline

## 1. Overview

To enhance the observability of the Milvus system, streamline the troubleshooting process, and provide consistent error responses to the SDK, the project has adopted a standardized error handling framework based on the `pkg/util/merr` package.

The core objectives are:
*   **Unified Error Codes**: Ensure every error is associated with a clear Milvus error code (e.g., `ErrParameterInvalid`).
*   **Error Classification**: Distinguish between **Input Error** (client-side/user input issues) and **System Error** (internal system anomalies).
*   **Error Chain Tracking**: Preserve the complete error stack and context from the bottom up.

---

## 2. Core Principles: Anti-Corruption Layer and Propagation

When writing code, please adhere to the following core guidelines:

### A. Boundary Calls — **Must Translate**
When calling **external packages** (e.g., Go standard libraries like `json`, `os`, or third-party libraries like `etcd`, `rocksdb`, `pulsar`), they return native `error` types.
*   **Requirement**: You must immediately wrap these into a Milvus standard error at the call site.
*   **Recommended Function**: Use `merr.WrapErrXxxErr(err, ...)`.

```go
// BAD: Directly returning a third-party error
if err := json.Unmarshal(data, &obj); err != nil {
    return err // ❌ Strictly forbidden to return native errors directly
}

// GOOD: Translate to a Milvus domain error
if err := json.Unmarshal(data, &obj); err != nil {
    return merr.WrapErrParameterInvalidErr(err, "fail to parse collection schema") // ✅
}
```

### B. Internal Propagation — **Only Append Context**
If the function you are calling is an internal Milvus function that already follows this specification (i.e., it returns a `merr`), you do not need to wrap it again with a specific error code.
*   **Requirement**: If you only need to pass the error up the chain and add some current context information (like an ID or name), use the generic wrap functions.
*   **Recommended Function**: Use `merr.Wrap(err, "context info")` or `merr.Wrapf(err, "...", args)`.
*   **Principle**: This preserves the underlying original error code (e.g., 1005) while appending the current execution context to the error message.

```go
// GOOD: Propagate and append context without changing the underlying error nature
res, err := m.internalManager.GetSegment(segmentID)
if err != nil {
    return merr.Wrapf(err, "failed to get segment for collection %d", collectionID) // ✅ The underlying ErrorCode is still preserved
}
```

---

## 3. Common Functions Guide

| Scenario | Recommended Function | Description |
| :--- | :--- | :--- |
| **Throwing a new error directly** | `merr.WrapErrXxxMsg("reason")` | Use when invalid logic is detected to directly return a standard error with an explanation. |
| **Wrapping an underlying native error** | `merr.WrapErrXxxErr(err, "format", args)` | Normalize an externally generated `error` to a specified Milvus error code, preserving the original error. |
| **Simply propagating and adding context** | `merr.Wrap(err, "msg")` | **Most common**. Used in internal call chains to preserve the underlying error code and only append debugging info. |
| **Explicitly marking error type** | `merr.WrapErrAsInputError(err)` | Forcefully mark an error as a user input error (used for monitoring classification). |

---

## 4. Error Classification: System vs. Input

In the `merr` definitions, errors are classified into two categories:
*   **Input Error**: Caused by client behavior (e.g., parameter format errors, collection does not exist, insufficient privileges).
    *   *Handling*: Typically does not trigger alerts; returned directly to the user via the SDK.
*   **System Error**: Internal system failures (e.g., disk IO failure, network timeout, out of memory, logic bugs).
    *   *Handling*: Triggers abnormal monitoring metrics and requires developer intervention for troubleshooting.

**Developer Responsibility**: When wrapping errors, choose the appropriate `Wrap` function based on business semantics. For example, if checking user parameters fails, be sure to use wrappers related to `merr.ErrParameterInvalid`.

---

## 5. How to Determine Error Types (Best Practices)

When you need to perform logic branching based on specific errors in your code, always use `merr.Is` or `errors.Is`:

```go
err := SomeFunction()
if merr.Is(err, merr.ErrCollectionNotFound) {
    // Specific remediation logic for when a collection does not exist
}

// Or checking the broad category
if merr.GetErrorType(err) == merr.InputError {
    // Log audit information, but do not trigger system alerts
}
```

---

## 6. The Safety Net: Automatic Normalization at the API Layer

To prevent native `error`s that slipped through (not wrapped by `merr`) from leaking directly to the client, we have implemented a fallback mechanism at the top API layer (Interceptor/Middleware):

1.  **Detection**: If the returned `error` chain does not contain any defined Milvus constant errors.
2.  **Alerting**: Logs a "Leaked unhandled native error" message.
3.  **Normalization**: Automatically wraps it as an `ErrServiceInternal` (Code 1) to return to the client, ensuring the strictness of the API structure.

---

## 7. Summary: Three Steps to Elegant Error Handling

1.  **Check the Bottom**: If the underlying call is to a standard/third-party library, use `merr.WrapErrXxxErr`.
2.  **Check the Current**: If the underlying call already returns a `merr`, simply use `merr.Wrap` to complete the context for the current function.
3.  **Think of the User**: Does the returned error code accurately tell the user whether to "modify parameters" or "contact the administrator"?
