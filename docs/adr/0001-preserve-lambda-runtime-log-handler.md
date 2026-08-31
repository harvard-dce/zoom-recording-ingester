# Lambda log records must be newline-terminated; reuse the runtime's handler

The garbled CloudWatch events of ZIP-104 (one event containing hundreds of
JSON records spanning many invocations) were caused by a log formatter whose
output lacked a trailing newline. The newline is the record terminator in the
lambda logging contract: the runtime's own formatters both end every record
with `"\n"` (see `awslambdaric.bootstrap._setup_logging` and
`lambda_runtime_log_utils.JsonFormatter`), and without it the platform
buffers and concatenates successive records — across invocations of a warm
container — flushing them as one giant malformed event. The unmaintained
`aws-lambda-logging` package had this defect; we reproduced it on demand in a
dev stack before adding the terminator to our in-repo formatter.

Two rules for `functions/utils/common.py`:

1. Any formatter used in lambda MUST terminate each record with `"\n"`.
2. `setup_logging` attaches our formatter/filter to the runtime's
   pre-installed handler (which is wired to the runtime's log sink) rather
   than replacing root handlers; a `StreamHandler` is created only when no
   handler exists (local scripts, tests).
