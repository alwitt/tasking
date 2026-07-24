# TODO

* [ ] Restrict Task Entry Deletion
  * Because Workflows rely on Tasks as execution history tracking, Task can't just be deleted at will. If a task was a workflow step executor, then it can't be deleted
  * When a workflow is being deleted, it will delete the tasks associated with executing the workflow steps as well.
